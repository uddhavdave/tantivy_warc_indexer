use std;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;

extern crate tantivy;
use crate::warc::DocJsonBuilder;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use entrez_rs::parser::pubmed::PubmedArticleSet;

pub async fn extract_records_and_add_to_json(
    mut reader: impl BufRead + Send,
    path: PathBuf,
) -> io::Result<()> {
    let out_file_path = path.with_extension("wka.json");
    let out_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&out_file_path)
        .await?;
    let mut writer = BufWriter::new(out_file);

    let mut doc = String::new();
    reader.read_to_string(&mut doc).expect("read file");
    let pm_parsed = PubmedArticleSet::read(&doc);
    let mut count = 0;
    let mut batch = Vec::new();
    for pubmed_article in pm_parsed.expect("parsed").articles {
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

        let mut doc = DocJsonBuilder::default();
        let article = pubmed_article
            .medline_citation
            .expect("medline_citation")
            .article
            .expect("article");
        if let Some(title) = article.title {
            doc.title(title);
        }
        if let Some(abstract_text) = article.abstract_text {
            for text in abstract_text.text {
                if let Some(value) = text.value {
                    doc.body(value);
                }
            }
        }
        doc.date("".into());
        doc.uri("".into());
        batch.push(doc.build().unwrap());
    }
    if batch.len() > 0 {
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
