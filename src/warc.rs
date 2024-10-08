use std;
use std::collections::HashMap;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;

extern crate tantivy;
use derive_builder::Builder;
use serde::Deserialize;
use serde::Serialize;
use std::io::Read;
use tantivy::Document;
use tantivy::Index;
use tantivy::IndexWriter;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

#[derive(Debug)]
enum WARCType {
    WarcInfo,
    Response,
    Resource,
    Request,
    Metadata,
    Revisit,
    Conversion,
    Continuation,
}

struct WARCRecord {
    warc_version: String,
    warc_type: WARCType,
    content_length: usize,
    payload: Vec<u8>,
    header: HashMap<String, String>,
    //        WARC-Type: warcinfo
    //        WARC-Date: 2020-04-10T14:25:56Z
    //        WARC-Filename: CC-MAIN-20200328074047-20200328104047-00001.warc.wet.gz
    //        WARC-Record-ID: <urn:uuid:9f7dbec8-10d4-4829-9a28-6edcf87d4b5a>
    //        Content-Type: application/warc-fields
}

fn read_record(reader: &mut dyn io::BufRead) -> Result<Option<WARCRecord>, std::io::Error> {
    let mut header: HashMap<String, String> = HashMap::new();
    for line in reader.lines() {
        let line = line?;
        let kv: Vec<_> = line.splitn(2, ':').map(|s| s.trim()).collect();
        match kv.as_slice() {
            [k, v] => header.insert(k.to_string(), v.to_string()),
            ["WARC/1.0"] => header.insert("WARC".to_string(), "1.0".to_string()),
            [""] =>
            // end of header
            {
                let warc_version = header
                    .get("WARC")
                    .expect("Header Field WARC version missing.")
                    .clone();
                let warc_type = match header
                    .get("WARC-Type")
                    .expect("Header Field WARC-Type missing")
                    .as_ref()
                {
                    "conversion" => WARCType::Conversion,
                    "warcinfo" => WARCType::WarcInfo,
                    "continuation" => WARCType::Continuation,
                    "revisit" => WARCType::Revisit,
                    "metadata" => WARCType::Metadata,
                    "request" => WARCType::Request,
                    "resource" => WARCType::Resource,
                    "response" => WARCType::Response,
                    other => panic!("Not a known WARCType: {}", other),
                };
                let content_length = header
                    .get("Content-Length")
                    .expect("Header Field Content-Length missing")
                    .parse::<usize>()
                    .expect("number for content-length");
                let mut payload = Vec::new();
                {
                    let mut payload_reader = reader.take(content_length as u64);
                    let bytes_read = payload_reader.read_to_end(&mut payload)?;
                    if bytes_read != content_length {
                        panic!("bytes_read != content_length")
                    }
                }

                // skip the record separator;
                reader.lines().next();
                reader.lines().next();

                return Ok(Some(WARCRecord {
                    warc_version,
                    warc_type,
                    content_length,
                    payload,
                    header,
                }));
            }
            [_, _, ..] => {
                eprintln!("ignoring unexpected multiple values");
                break;
            }
            [_] => {
                eprintln!("ignoring unexpected single value");
                break;
            }
            [] => {
                eprintln!("ignoring unexpected empty line");
                break;
            }
        };
    }

    return Ok(None);
}

#[derive(Debug, Serialize, Clone, Deserialize, Builder)]
pub struct DocJson {
    pub uri: String,
    pub title: String,
    pub body: String,
    pub date: String,
}

pub async fn extract_records_and_push_to_quickwit(
    mut reader: impl BufRead + Send,
    out_file_path: PathBuf,
) -> io::Result<()> {
    let mut count = 0;
    let mut batch = Vec::new();
    let out_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&out_file_path)
        .await?;
    let mut writer = BufWriter::new(out_file);
    while let Some(record) = read_record(&mut reader)? {
        if batch.len() > 10 {
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

        match record.warc_type {
            WARCType::WarcInfo => {
                //eprintln!("{}", String::from_utf8(record.payload).expect("warcinfo in UTF-8"));
            }
            WARCType::Conversion => {
                let body = String::from_utf8(record.payload).expect("convert to utf8 failed");
                // create a json builder
                // parse the body into a json object
                count += 1;
                if count % 1000 == 0 {
                    eprint!(".");
                }
                let body = body.to_string();
                let uri = record
                    .header
                    .get("WARC-Target-URI")
                    .expect("get uri")
                    .to_string();
                let count = body.chars().count() / 10;
                let title_len = body
                    .char_indices()
                    .nth(std::cmp::min(10, count) as usize)
                    .unwrap()
                    .0;
                let title = body.split_at_checked(title_len).unwrap().0.to_string();
                let date = record
                    .header
                    .get("WARC-Date")
                    .expect("get date")
                    .to_string();

                let doc = DocJson {
                    uri,
                    title,
                    body,
                    date,
                };
                batch.push(doc);
                // tx.send(doc).await.unwrap();
            }
            _ => {
                //eprintln!("ignoring record type: {:?}", record.warc_type);
            }
        }
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

// pub async fn send_to_quickwi(mut rx: Receiver<DocJson>) {
//     let url = "http://localhost:7280/api/v1/common_crawl/ingest?commit=force";
//     let batch = 1000;

//     while let Some(doc) = rx.recv().await {
//         let doc_json = serde_json::to_string(&doc).expect("json serialization failed");
//         let client = reqwest::Client::new()
//             .post(url)
//             .header("Content-Type", "application/json");
//         let resp = client.body(doc_json).send().await;
//         let _status = resp.unwrap().status();
//     }
// }

pub fn extract_records_and_add_to_index(
    index: &Index,
    index_writer: &IndexWriter,
    reader: &mut dyn BufRead,
) -> io::Result<()> {
    let schema = index.schema();
    let schema_uri = schema.get_field("uri").unwrap();
    let schema_title = schema.get_field("title").unwrap();
    let schema_body = schema.get_field("body").unwrap();
    let schema_date = schema.get_field("date").unwrap();

    let mut count = 0;
    while let Some(record) = read_record(reader)? {
        match record.warc_type {
            WARCType::WarcInfo => {
                //eprintln!("{}", String::from_utf8(record.payload).expect("warcinfo in UTF-8"));
            }
            WARCType::Conversion => {
                count += 1;
                if count % 1000 == 0 {
                    eprint!(".");
                }

                let body = std::str::from_utf8(&record.payload).expect("convert to utf8 failed");
                let mut doc = Document::default();
                doc.add_text(
                    schema_uri,
                    &record.header.get("WARC-Target-URI").expect("get uri"),
                );
                doc.add_text(
                    schema_date,
                    &record.header.get("WARC-Date").expect("get date"),
                );
                doc.add_text(schema_body, body);
                doc.add_text(schema_title, body.lines().next().expect("title"));
                index_writer.add_document(doc);
            }
            _ => (),
        }
    }

    println!("\nTotal Records of WARC file processed: {}", count);
    Ok(())
}
