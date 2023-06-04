import weaviate from "weaviate-ts-client";
import fs from "fs";
import httpCli from "https";
import request from "request-promise";
import axios from "axios";
var startTime = performance.now();
const weavClassName = "Socrsseoss";

async function downloadImage(url) {
  const response = await axios.get(url, { responseType: "arraybuffer" });

  // Convert the image data to base64
  const b64 = Buffer.from(response.data, "binary").toString("base64");
  return b64;
}

const client = weaviate.client({
  scheme: "http",
  host: "localhost:8080",
});

const schemaConfig = {
  class: weavClassName,
  vectorizer: "img2vec-neural",
  vectorIndexType: "hnsw",
  moduleConfig: {
    "img2vec-neural": {
      imageFields: ["image"],
    },
  },
  properties: [
    {
      name: "image",
      dataType: ["blob"],
    },
    {
      name: "text",
      dataType: ["string"],
    },
  ],
};

await client.schema.classCreator().withClass(schemaConfig).do();
//await client.schema.classCreator().withClass(schemaConfig).do();

const schemaRes = await client.schema.getter().do();

async function removeClass(className) {
  // Create a new instance of the Weaviate client
  try {
    await client.schema.classDeleter().withClassName(className).do();

    console.log(`Class '${className}' successfully removed.`);
  } catch (error) {
    console.error("Error:", error);
  }
}

const b64 = await downloadImage(
  "https://www.vildmarkskallare.se/storage/product_images/8/tuotesivu_BearskinHoddeyhupparipunainen_7350065731368_294d208af091499b289cdf92f93998aa_1.png"
);

async function addImage(imgUrl) {
  const img = fs.readFileSync("./yellowjacket.jpg");

  const b64 = await downloadImage(imgUrl);

  let res = await client.data
    .creator()
    .withClassName(weavClassName)
    .withProperties({
      image: b64,
      text: "matrix meme",
    })
    .do();
  var endTime = performance.now();
  console.log(`Before imageSort ${(endTime - startTime) / 1000} seconds`);
  return res;
}
var promises = [];
for (let i = 0; i < 10; i++) {
  promises[i] = addImage(
    "https://www.vildmarkskallare.se/storage/product_images/8/tuotesivu_BearskinHoddeyhupparipunainen_7350065731368_294d208af091499b289cdf92f93998aa_1.png"
  );
}

const results = await Promise.allSettled(promises);
//console.log(results);

const test = Buffer.from(fs.readFileSync("./yellowjacket.jpg")).toString(
  "base64"
);

const resImage = await client.graphql
  .get()
  .withClassName(weavClassName)
  .withFields(["image"])
  .withNearImage({ image: test })
  .do();

// Write result to filesystem
const result = resImage.data.Get[weavClassName][0].image;
console.log(resImage.data.Get[weavClassName].length);
var endTime = performance.now();

console.log(`Before imageSort ${(endTime - startTime) / 1000} seconds`);
//fs.writeFileSync("./result.jpg", result, "base64");
await removeClass(weavClassName);
