use std::{
    collections::VecDeque,
    error::Error,
    ffi::OsStr,
    fs::{ FileType, Metadata },
    marker::PhantomData,
    path::{ Path, PathBuf },
    str::FromStr,
    sync::{ Arc, Weak },
};
use clap::{Parser, command};
use serde::{ Deserialize, Serialize };
use serde_json::{ self, to_string_pretty };
use tokio::{
    io::{ AsyncWriteExt, stdout },
    join,
    sync::{ RwLock, mpsc::{ UnboundedReceiver, UnboundedSender, channel } },
};

fn main() {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(amain());
}
pub async fn amain() {
    let init = Init::parse();
    let dir = PathBuf::from(init.initial_path);
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut crawler = Crawler { elements_sender: sender };
    let mut writer = Writer { input: receiver };
    let (_, _) = join!(crawler.crawl(dir.as_path(), init.max_depth), writer.run());
}

#[derive(Debug, Parser)]
#[command(about,)]
pub struct Init {
    #[arg(default_value=".")]
    pub initial_path: PathBuf,
    #[arg(short='d', long)]
    pub max_depth: Option<u16>,
}

impl Writer {
    pub async fn run(&mut self) {
        let mut stdout = stdout();
        stdout.write_all(b"name    type\n").await.unwrap();
        async fn print_element(
            element: &OwnedFsElement,
            stdout: &mut tokio::io::Stdout,
            is_first: &mut bool
        ) {
            if *is_first {
                *is_first = false;
            } else {
                stdout.write_all(b"\n").await.unwrap();
            }

            let row = element.make_row();
            let row_str = format!("{}    {}", row.path.display(), row.r#type.as_str());
            stdout.write_all(row_str.as_bytes()).await.unwrap();
        }
        let mut is_first = true;

        while let Some(weak_element) = self.input.recv().await {
            if let Some(element) = weak_element.upgrade() {
                print_element(&element, &mut stdout, &mut is_first).await;
            }
        }
        stdout.flush().await.unwrap();
        // stdout.write_all(b"]").await.unwrap();
    }
}
type ChannelData = WeakFsElement;
type Sender     = UnboundedSender<ChannelData>;
type Receiver = UnboundedReceiver<ChannelData>;
pub struct Input {
    starting_path: String,
}

pub struct Crawler {
    elements_sender: UnboundedSender<WeakFsElement>,
}

pub type WriterElement = Weak<OwnedFsElement>;
impl Crawler {
    pub async fn crawl(self, dir_path: &Path, depth: Option<u16>) -> Result<OwnedFsElement, anyhow::Error> {
        async fn crawl_inner(
            dir_path: &Path,
            parent: Option<Weak<FsDir>>,
            sender: &UnboundedSender<WeakFsElement>,
            remaining_depth: Option<u16>,
        ) -> Result<Arc<FsDir>, anyhow::Error> {
            let mut entries = tokio::fs::read_dir(dir_path).await?;

            let dir = Arc::new(FsDir {
                name: dir_path
                    .file_name()
                    .unwrap_or(dir_path.as_os_str())
                    .to_string_lossy()
                    .into_owned(),
                r#type: FsFileType::Directory,
                children: RwLock::new(Vec::new()),
                parent,
            });

            let dir_cell = Arc::downgrade(&dir);

            while let Some(entry) = entries.next_entry().await? {
                let open_entry_task = async {
                    let entry_type = entry.file_type().await?;
                    let entry_name = entry.file_name();
                    let entry = {
                        if entry_type.is_dir() {
                            if let Some(remaining_depth) = remaining_depth {
                                if remaining_depth == 0 {
                                    OwnedFsElement::Leaf(Arc::new(FsLeaf{
                                        name: entry_name.to_string_lossy().into_owned(),
                                        r#type: FsFileType::Directory,
                                        parent: Some(dir_cell.clone()),
                                    }))
                                } else {
                                    let dir = Box::pin(
                                        crawl_inner(
                                            entry.path().as_path(),
                                            Some(dir_cell.clone()),
                                            sender,
                                            Some(remaining_depth - 1),
                                        )
                                    ).await?;
                                    OwnedFsElement::Dir(dir)
                                }
                            } else {
                                let dir = Box::pin(
                                    crawl_inner(
                                        entry.path().as_path(),
                                        Some(dir_cell.clone()),
                                        sender,
                                        None,
                                    )
                                ).await?;
                                OwnedFsElement::Dir(dir)
                            }
                        } else {
                            let metadata = entry.metadata().await?;
                            OwnedFsElement::Leaf(
                                Arc::new(FsLeaf {
                                    name: entry_name.to_string_lossy().into_owned(),
                                    // size: metadata.len() as usize,
                                    r#type: entry_type.into(),
                                    parent: Some(dir_cell.clone()),
                                })
                            ) // handle file
                        }
                    };
                    Result::<_, anyhow::Error>::Ok(entry)
                };
                if let Ok(entry) = open_entry_task.await {
                    let weak_entry = entry.downgrade();
                    sender.send(weak_entry).unwrap();
                    dir.children.write().await.push(entry);
                } else {
                    continue;
                }
            }
            Ok(dir)
        }
        let owned_dir = crawl_inner(dir_path, None, &self.elements_sender, depth).await?;
        drop(self.elements_sender);
        Ok(OwnedFsElement::Dir(owned_dir))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OutputRow {
    path: PathBuf,
    r#type: FsFileType,
}
pub struct Writer {
    input: UnboundedReceiver<WeakFsElement>,
}

#[derive(Debug)]
pub enum OwnedFsElement {
    Dir(Arc<FsDir>),
    Leaf(Arc<FsLeaf>),
}

impl OwnedFsElement {
    pub fn downgrade(&self) -> WeakFsElement {
        match self {
            OwnedFsElement::Dir(dir) => WeakFsElement::Dir(Arc::downgrade(dir)),
            OwnedFsElement::Leaf(leaf) => WeakFsElement::Leaf(Arc::downgrade(leaf)),
        }
    }
}
impl WeakFsElement {
    pub fn upgrade(&self) -> Option<OwnedFsElement> {
        match self {
            WeakFsElement::Dir(dir) => dir.upgrade().map(OwnedFsElement::Dir),
            WeakFsElement::Leaf(leaf) => leaf.upgrade().map(OwnedFsElement::Leaf),
        }
    }
}

#[derive(Debug)]
pub enum WeakFsElement {
    Dir(Weak<FsDir>),
    Leaf(Weak<FsLeaf>),
}

pub type Parent = Weak<FsDir>;

#[derive(Debug)]
pub struct FsDir {
    pub name: String,
    pub r#type: FsFileType,
    pub children: RwLock<Vec<OwnedFsElement>>,
    pub parent: Option<Weak<FsDir>>,
}

#[derive(Debug)]
pub struct FsLeaf {
    pub name: String,
    pub r#type: FsFileType,
    // pub size: usize,

    pub parent: Option<Weak<FsDir>>,
}

impl HasBaseFsData for OwnedFsElement {
    fn file_type(&self) -> FsFileType {
        match self {
            OwnedFsElement::Dir(dir) => dir.file_type(),
            OwnedFsElement::Leaf(leaf) => leaf.file_type(),
        }
    }
    fn name(&self) -> &str {
        match self {
            OwnedFsElement::Dir(dir) => dir.name(),
            OwnedFsElement::Leaf(leaf) => leaf.name(),
        }
    }
    fn parent(&self) -> Option<Weak<FsDir>> {
        match self {
            OwnedFsElement::Dir(dir) => dir.parent(),
            OwnedFsElement::Leaf(leaf) => leaf.parent(),
        }
    }
}

impl HasBaseFsData for FsLeaf {
    fn file_type(&self) -> FsFileType {
        self.r#type.clone()
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn parent(&self) -> Option<Weak<FsDir>> {
        self.parent.clone()
    }
}
impl HasBaseFsData for FsDir {
    fn file_type(&self) -> FsFileType {
        self.r#type.clone()
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn parent(&self) -> Option<Weak<FsDir>> {
        self.parent.clone()
    }
}

trait HasBaseFsData {
    fn file_type(&self) -> FsFileType;
    fn name(&self) -> &str;
    fn parent(&self) -> Option<Weak<FsDir>>;

    fn make_row(&self) -> OutputRow where Self: Sized {
        let path = {
            let mut path = PathBuf::new();
            let mut walker = self.path_walker();
            while let Some(part) = walker.next() {
                path.push(part);
            }
            path
        };
        OutputRow {
            path,
            r#type: self.file_type(),
        }
    }
    fn path_walker(&self) -> FsElementParentPathIter<'_, Self> where Self: Sized {
        FsElementParentPathIter::new(self)
    }
}

struct FsElementParentPathIter<'a, Base: HasBaseFsData> {
    stack: VecDeque<Arc<FsDir>>,
    start: &'a Base,
    index: usize,
}

impl<'a, Base: HasBaseFsData> FsElementParentPathIter<'a, Base> {
    fn new(start: &'a Base) -> Self {
        let mut stack = VecDeque::new();
        let mut current = start.parent();
        while let Some(weak_dir) = current {
            if let Some(dir) = weak_dir.upgrade() {
                stack.push_front(dir.clone());
                current = dir.parent();
            } else {
                break;
            }
        }
        Self {
            stack,
            start,
            index: 0,
        }
    }

    fn next<'b>(&'b mut self) -> Option<&'b str> {
        match self.index {
            i if i < self.stack.len() => {
                let el = self.stack.get(i).unwrap();
                self.index += 1;
                Some(&el.name)
            }
            i if i == self.stack.len() => {
                self.index += 1;
                Some(self.start.name())
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsFileType {
    File,
    Directory,
    NoIdeaLmao,
}
impl FsFileType {
    fn as_str(&self) -> &str {
        match self {
            FsFileType::File => "file",
            FsFileType::Directory => "dir",
            FsFileType::NoIdeaLmao => "unknown",
        }
    }
}

impl Into<FsFileType> for FileType {
    fn into(self) -> FsFileType {
        if self.is_dir() {
            FsFileType::Directory
        } else if self.is_file() {
            FsFileType::File
        } else {
            FsFileType::NoIdeaLmao
        }
    }
}
