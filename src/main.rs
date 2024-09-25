//#![feature(associated_type_bounds)]
use std;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::PathBuf;

use docopt::Docopt;
extern crate tantivy;
use flate2::read::MultiGzDecoder;
use tantivy::Index;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio::sync::Semaphore;
use warc::send_to_quickwit;
use warc::DocJson;

mod pubmed;
mod warc;
mod wikipedia_abstract;

const USAGE: &'static str = "
WARC Indexer

Usage:
  warc_parser [-t <threads>] [--from <from>] [--to <to>] -s <format> <index> <warc_dir>
  warc_parser (-h | --help)

Options:
  -h --help      Show this help
  -s <source>    type of source files (WARC or ENTREZ or WIKIPEDIA_ABSTRACT)
  -t <threads>   number of threads to use, default 4
  --from <from>  skip files until from
  --to <to>      skip files after to
";

#[derive(Debug)]
enum SourceType {
    WARC,
    WIKIPEDIA_ABSTRACT,
    ENTREZ,
}

#[derive(Debug)]
struct Args {
    arg_index: Vec<String>,
    arg_warc_dir: Vec<String>,
    flag_threads: usize,
    flag_source: SourceType,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Docopt::new(USAGE)
        .and_then(|d| d.argv(std::env::args().into_iter()).parse())
        .unwrap_or_else(|e| e.exit());

    let source_type = args.get_str("-s").to_string();
    let index_dir = args.get_str("<index>");
    let warc_dir = args.get_str("<warc_dir>");
    let threads = args.get_str("-t");
    let from = args.get_str("--from").parse::<usize>().unwrap_or(0);
    let to = args.get_str("--to").parse::<usize>().unwrap_or(usize::MAX);
    let nthreads: usize = threads.parse().unwrap_or(4);
    const PER_THREAD_BUF_SIZE: usize = 600 * 1024 * 1024;

    println!("Only indexing files: {} - {}", from, to);
    println!("Index dir: {:?}", index_dir);
    println!("Warc dir: {:?}", warc_dir);
    println!("Threads: {:?}", nthreads);
    println!("");

    let (tx, rx) = tokio::sync::mpsc::channel::<DocJson>(1000);

    let mut numfiles = 0;
    let mut tasks = Vec::new();
    let semaphore = std::sync::Arc::new(Semaphore::new(16));

    let sender = tokio::spawn(async move {
        send_to_quickwit(rx).await;
    });

    while let Some(path) = tokio::fs::read_dir(warc_dir)
        .await
        .unwrap()
        .next_entry()
        .await
        .unwrap()
    {
        numfiles += 1;
        if numfiles < from || numfiles > to {
            continue;
        }
        let filename = path.path().clone();

        let source_type_clone = source_type.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let tx_clone = tx.clone();
        tasks.push(tokio::task::spawn(async move {
            let file = File::open(&filename).unwrap();

            eprintln!("{}\t{}", numfiles, filename.to_string_lossy());
            match filename.extension() {
                Some(extension) => {
                    if extension == OsStr::new("gz") {
                        println!("gzipped {}", source_type_clone);
                        match source_type_clone.as_str() {
                            "WARC" | "WIKIPEDIA_ABSTRACT" | "ENTREZ" => {
                                warc::extract_records_and_push_to_quickwit(
                                    &mut io::BufReader::with_capacity(
                                        PER_THREAD_BUF_SIZE,
                                        MultiGzDecoder::new(file),
                                    ),
                                    tx_clone,
                                )
                                .await
                                .unwrap()
                            }
                            _ => eprintln!("Unknown source type {}", source_type_clone),
                        }
                    } else if extension == OsStr::new("wet") {
                        warc::extract_records_and_push_to_quickwit(
                            &mut io::BufReader::with_capacity(PER_THREAD_BUF_SIZE, file),
                            tx_clone,
                        )
                        .await
                        .unwrap();
                    } else {
                        eprintln!("Skip file, neither wet nor gz");
                    }
                }
                None => eprintln!("Skip file, neither wet nor gz"),
            }
            drop(permit);
        }))
    }

    let _ = tokio::join!(sender);
    Ok(())
}
