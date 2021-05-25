extern crate rusoto_core;
extern crate rusoto_s3;
extern crate tokio;

use std::env;

use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

use rusoto_core::{Region};
use rusoto_s3::{
    S3,
    S3Client,
    GetObjectRequest,
    ListObjectsV2Request
};


type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T,E = Error> = std::result::Result<T, E>;

enum Command<'a> {
    Ls(S3Ctx<'a>),
    Cat(S3Ctx<'a>),
    Unknown
}

impl<'a> Command<'a> {
    fn from(ctx: S3Ctx<'a>, s: &str) -> Self {
        match s {
          "ls" => Command::Ls(ctx),
          "cat" => Command::Cat(ctx),
          _ => Command::Unknown
        }
    }

    async fn run(&self) -> () {
       match &*self {
          Command::Ls(ctx) => ctx.list_objects().await,
          Command::Cat(ctx) => ctx.cat_objects().await,
          Command::Unknown => ()
       }
    }
}

struct S3Ctx<'a> {
    client: &'a S3Client,
    bucket: &'a str,
    key: &'a str,
    filter: Option<&'a str>,
    ng_filter: Option<&'a str>
}

impl<'a> S3Ctx<'a> {
    fn new(
        _client: &'a S3Client,
        _bucket: &'a str,
        _key: &'a str,
        _filter: Option<&'a str>,
        _ng_filter: Option<&'a str>
     ) -> Self {
        S3Ctx {
          client: _client,
          bucket: _bucket,
          key: _key,
          filter: _filter,
          ng_filter: _ng_filter
        }
    }

    async fn keys(&self) -> Result<Vec<String>> {
        let list_obj_req = ListObjectsV2Request {
            bucket: self.bucket.to_owned(),
            start_after: Some(self.key.to_owned()),
            ..Default::default()
        };

        let result = self.client.list_objects_v2(list_obj_req).await;
        match result {
            Ok(list_objests_v2_output) => Ok(list_objests_v2_output.contents
              .map(|objs| objs.iter()
                .map(|obj| obj.key.as_ref().map(|s| String::from(s)).unwrap_or("".to_string()))
                .filter(|s| s.contains(self.key))
                .filter(|s| if self.filter.is_some() { s.contains(self.filter.unwrap_or("")) } else { true })
                .filter(|s| if self.ng_filter.is_some() { !s.contains(self.ng_filter.unwrap_or("")) } else { true })
                .collect::<Vec<String>>()
              ).unwrap_or(Vec::new())),
            Err(err) =>  Err(Box::new(err))
        }
    }

    async fn get_object(&self, filename: &str) -> Vec<u8> {
        let get_req = GetObjectRequest {
            bucket: self.bucket.to_owned(),
            key: filename.to_owned(),
            ..Default::default()
        };

        let result = self.client
          .get_object(get_req)
          .await
          .expect("Couldn't GET object");

        let mut stream = result
          .body
          .unwrap()
          .into_async_read();

        let mut body = Vec::new();

        stream
          .read_to_end(&mut body)
          .await
          .unwrap();

        body

    }

    pub async fn list_objects(&self) -> () {
        let mut stdout = io::stdout();
        let s3keys_result = self.keys().await;
        match s3keys_result {
          Err(err) => println!("{:?}", err),
          Ok(v) => {
            for key in v {
              stdout
                .write_all(format!("{}\n", key)
                .as_bytes())
                .await
                .unwrap();
            }
          }
        }
    }

    pub async fn cat_objects(&self) -> () {
        let mut stdout = io::stdout();
        let s3keys_result = self.keys().await;
        match s3keys_result {
          Err(err) => println!("{:?}", err),
          Ok(v) => {
            for key in v {
                let b = self.get_object(&key).await;
                stdout.write_all(&b).await.unwrap();
            }
          }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4  {
        return println!("Usage: {} <command> <bucket_name> <key> <filter>", args[0])
    }

    let command = args[1].as_str();
    let bucket_name = args[2].as_str();
    let key = args[3].as_str();
    let filter = if args.len() > 4 { Some(args[4].as_str()) } else { None };
    let ng_filter= if args.len() > 5 { Some(args[5].as_str()) } else { None };

    let client = S3Client::new(Region::ApNortheast1);
    let ctx = S3Ctx::new(&client, bucket_name, key, filter, ng_filter);

    Command::from(ctx, command)
      .run()
      .await
}
