use std;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;

extern crate tantivy;
use crate::warc::DocJson;
use serde;
use serde::{Deserialize, Serialize};
use serde_xml_rs::from_str;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Feed {
    doc: Vec<DocEntry>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct DocEntry {
    r#abstract: String,
    //links : String,
    title: String,
    url: String,
}

pub async fn extract_records_and_add_to_json(
    mut reader: impl BufRead + Send,
    path: PathBuf,
) -> io::Result<()> {
    // convert the path to wka.json
    let out_file_path = path.with_extension("wka.json");
    let out_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&out_file_path)
        .await?;
    let mut writer = BufWriter::new(out_file);
    let mut src = String::new();
    reader.read_to_string(&mut src).expect("read file");
    let feed: Feed = from_str(&src).unwrap();

    let mut batch = Vec::new();
    let mut count = 0;
    for doc_entry in feed.doc {
        count += 1;
        if count % 1000 == 0 {
            eprint!(".");
        }

        if batch.len() > 1000 {
            let docs = batch
                .iter()
                .map(|doc| serde_json::to_string(&doc).unwrap())
                .collect::<Vec<String>>();
            // join on newline
            let blob = docs.join("\n");
            let blob = blob.as_bytes();
            writer.write(blob).await.unwrap();
            writer.flush().await.unwrap();
            batch.clear();
        }

        let doc = DocJson {
            title: doc_entry.title,
            body: doc_entry.r#abstract,
            uri: doc_entry.url,
            date: "".into(),
        };
        batch.push(doc);
    }

    if batch.len() > 0 {
        // send to quickwit
        // send_to_quickwi(batch).await;
        let docs = batch
            .iter()
            .map(|doc| serde_json::to_string(&doc).unwrap())
            .collect::<Vec<String>>();
        // join on newline
        let blob = docs.join("\n");
        let blob = blob.as_bytes();
        writer.write(blob).await.unwrap();
        writer.flush().await.unwrap();
        batch.clear();
    }
    println!("\nTotal Records of WARC file processed: {}", count);
    Ok(())
}
