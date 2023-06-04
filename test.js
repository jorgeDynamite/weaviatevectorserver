import request from "request-promise";
import axios from "axios";
import weaviate from "weaviate-ts-client";
const client = weaviate.client({
  scheme: "http",
  host: "localhost:8080",
});

const schemaConfig = {
  class: "EEE",
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

async function addImage(imgUrl) {
  const img = fs.readFileSync("./yellowjacket.jpg");

  const b64 = await downloadImage(
    "https://www.vildmarkskallare.se/storage/product_images/8/tuotesivu_BearskinHoddeyhupparipunainen_7350065731368_294d208af091499b289cdf92f93998aa_1.png"
  );

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

async function downloadImage(url) {
  const response = await axios.get(url, { responseType: "arraybuffer" });

  // Convert the image data to base64
  const b64 = Buffer.from(response.data, "binary").toString("base64");
  return b64;
}
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
var test_image_list = [
  "https://encrypted-tbn2.gstatic.com/shopping?q=tbn:ANd9GcSTDAPnQlUIvv8JJdk9KPpRT7Pi7HLJ3HFByma4JOrElqpLkORVH9ky6u2D4b2Kwk5pxn9zMh75pmT5F-l_j_lg0fR53l_XC59Nx83TydyHzIIjhv2eaZlV&usqp=CAE",
  "https://encrypted-tbn1.gstatic.com/shopping?q=tbn:ANd9GcSQ-QQGRJQtTpQcOZ8FGhe9dJ20X4GcKwPEjAF_RLm7TbDLtyBnOjCa-FsoMFiyxoekBmhwwWbsdnNg1vEB8JAhzul3_aULNNBwoKiDH5rpYrQGQLMnV54D&usqp=CAE",
  "https://encrypted-tbn0.gstatic.com/shopping?q=tbn:ANd9GcRnY671PkhGV69QBf8_u8-L0RiGtjzYkhrlrwXSPREQijY8IJO0ZMtLRJKUB3zklayTxskS9rHmY4hIYkHiSFYzU_SbE1D9ATrVa_EGJFJdepdQ1QddSI1Fdw&usqp=CAE",
  "https://encrypted-tbn2.gstatic.com/shopping?q=tbn:ANd9GcSTDAPnQlUIvv8JJdk9KPpRT7Pi7HLJ3HFByma4JOrElqpLkORVH9ky6u2D4b2Kwk5pxn9zMh75pmT5F-l_j_lg0fR53l_XC59Nx83TydyHzIIjhv2eaZlV&usqp=CAE",
  "https://encrypted-tbn1.gstatic.com/shopping?q=tbn:ANd9GcSQ-QQGRJQtTpQcOZ8FGhe9dJ20X4GcKwPEjAF_RLm7TbDLtyBnOjCa-FsoMFiyxoekBmhwwWbsdnNg1vEB8JAhzul3_aULNNBwoKiDH5rpYrQGQLMnV54D&usqp=CAE",
  "https://encrypted-tbn3.gstatic.com/shopping?q=tbn:ANd9GcQVxG5n13XI5Y2ota46YTlTfl8GqtxWZfPUZ2K53BLCyaCW8iUPnfU74hMBy7dNNUfOtzuG-zK_t5AYnteAT0T95qZnqutzv7yr37qAgba6qRZAfCCLw3KEeg&usqp=CAE",
  "https://encrypted-tbn1.gstatic.com/shopping?q=tbn:ANd9GcQdqNhMZjhO--oFGDm5hy4Pvin8V2WbXqjMtNC9aW2O0-650oFUC0-sjKBtzY6ZbpGcrY_td-2mLiek-uhBZkKaZQav15TwQ8U_XEzwz2KkDrRj-hCWmlV9&usqp=CAE",
  "https://encrypted-tbn3.gstatic.com/shopping?q=tbn:ANd9GcQiTowzcFEgP7uNubL12oWjtFawi_Rd-Aa9dVY9bJFzsk_i04zUOT03hfl9ah9Nj7yfITxbBsKh7nbTih_PL4rHsui1Nzm9v_sRNf4oFhd7F7Mocvd9f9or&usqp=CAE",
  "https://m.media-amazon.com/images/I/61isFBFKuXL._AC_UL320_.jpg",
  "https://m.media-amazon.com/images/I/7130bFw41oL._AC_UL400_.jpg",
  "https://m.media-amazon.com/images/I/61kDpJtSFNL._AC_UL320_.jpg",
  "https://m.media-amazon.com/images/I/91pdpqiL+pL._AC_UL400_.jpg",
  "https://m.media-amazon.com/images/I/51LTOabi1OL._AC_UL320_.jpg",
  "https://m.media-amazon.com/images/I/91LJaQPgyqL._AC_UL400_.jpg",
  "https://m.media-amazon.com/images/I/812gVtHiylL._AC_UL320_.jpg",
  "https://m.media-amazon.com/images/I/91g3eCCrmKL._AC_UL400_.jpg",
  "http://ae01.alicdn.com/kf/Hff473b148882457cab5a315bf3e0f543u.jpg",
  "http://ae01.alicdn.com/kf/H15a129e37fc9476fb405e2ed71292b19t.jpg",
  "http://ae01.alicdn.com/kf/Hc9b302e1b805458e96ebd65c97a923942.jpg",
  "http://ae01.alicdn.com/kf/Hb9c744a61c424bf9b84e0bef2fb68b467.jpg",
  "http://ae01.alicdn.com/kf/Sc4c29c26884a40989263b7f9977b7adfc.jpg",
  "http://ae01.alicdn.com/kf/Scb01fae09baa466f92571340cd8a8db7M.jpg",
  "http://ae01.alicdn.com/kf/H4bd581dfffed4cad97cd77a45e68beffR.jpg",
  "http://ae01.alicdn.com/kf/S43fb0efc59764906989ce4639009e048W.jpg",
];
async function batchImport() {
  console.log("Starting BatchImport");
  var startTime = performance.now();
  // Get the data from the data.json file

  // Prepare a batcher
  let batcher = client.batch.objectsBatcher();
  let counter = 0;
  const b64 = await downloadImage(
    "https://encrypted-tbn2.gstatic.com/shopping?q=tbn:ANd9GcSTDAPnQlUIvv8JJdk9KPpRT7Pi7HLJ3HFByma4JOrElqpLkORVH9ky6u2D4b2Kwk5pxn9zMh75pmT5F-l_j_lg0fR53l_XC59Nx83TydyHzIIjhv2eaZlV&usqp=CAE"
  );
  var promises = [];
  for (let i = 0; i < test_image_list.length; i++) {
    promises[i] = await downloadImage(test_image_list[i]);
  }
  const results = promises;
  let batchSize = 10;
  for (let index = 0; index < results.length; index++) {
    console.log(index);
    // Construct an object with a class and properties 'answer' and 'question'
    const obj = {
      class: "EEE",
      properties: {
        image: results[index],
        text: "New Images" + index.toString(),
      },
    };

    // add the object to the batch queue
    batcher = batcher.withObject(obj);

    // When the batch counter reaches batchSize, push the objects to Weaviate
    if (counter++ == batchSize) {
      try {
        console.log("Do batcher");
        // flush the batch queue
        await batcher.do();

        // restart the batch queue
        counter = 0;
        batcher = client.batch.objectsBatcher();
        var endTime = performance.now();
        console.log(
          `${index} batch finished ${(endTime - startTime) / 1000} seconds`
        );
      } catch (error) {
        console.error("Error during batch import:", error);
      }
    }
  }

  // Flush the remaining objects
  batcher
    .do()
    .then(async (res) => {
      console.log(res.length);
      var endTime = performance.now();
      console.log(`End time ${(endTime - startTime) / 1000} seconds`);

      const test = await downloadImage(
        "https://m.media-amazon.com/images/I/61QAidyQoQL._AC_UF1000,1000_QL80_.jpg"
      );
      const resImage = await client.graphql
        .get()
        .withClassName("EEE")
        .withFields(["text"])
        .withNearImage({ image: test })
        .do();
      console.log("Result array length" + resImage.data.Get["EEE"].length);
      removeClass("EEE");
    })
    .catch((err) => {
      console.log("Error get batch");
      console.error(err);
    });
}

async function removeClass(className) {
  // Create a new instance of the Weaviate client
  try {
    await client.schema.classDeleter().withClassName(className).do();

    console.log(`Class '${className}' successfully removed.`);
  } catch (error) {
    console.error("Error:", error);
  }
}
batchImport();

/**/
