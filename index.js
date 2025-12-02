import fs from 'fs';
import { parse } from 'csv-parse';
import axios from 'axios';
import mongoose from 'mongoose';

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost/mtg';
const CSV_FILE = process.env.MANABOX_CSV || 'MTG.csv';
const BATCH_SIZE = 75;
const USER_AGENT = 'ManaboxToMongoDB/1.0';
const SCRYFALL_TIME_LIMIT = 100;
const SCRYFALL_COLLECTION_ENDPOINT = 'https://api.scryfall.com/cards/collection';

let db = null;
let MagicCardSchema = null;
let MagicCard = null;
let MagicCollectionSchema = null;
let MagicCollection = null;
let totalCards = 0;
let totalPrice = 0;

async function exit(err) {
  console.error('Failed to import data');
  console.error(err);

  if (db) {
    await db.disconnect();
  }

  process.exit(1);
}

function formatCurrency(amount) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency', currency: 'USD'
  }).format(amount);
}

async function readCsv() {
  console.log(`Reading data from ${CSV_FILE}`);
  const records = [];
  const parser = fs
    .createReadStream(CSV_FILE)
    .on('error', (err) => {
      exit(err);
    })
    .pipe(parse({
      delimiter: ',',
      from_line: 2,
      relax_column_count: true,
      record_delimiter: '\n',
      skip_empty_lines: true,
      ltrim: true,
      rtrim: true,
    }));

  for await (const row of parser) {
    const record = {
      foil: row[4] === 'normal' ? false : true,
      quantity: row[6],
      manaboxId: row[7],
      scryfallId: row[8],
      price: row[9] * 100
    };
    records.push(record);
  }

  return records;
}

async function saveCards(cards) {
  console.log('Saving batch of cards');

  const res = await MagicCard.insertMany(cards);
}

async function saveCollection() {
  console.log('Saving collection');

  const collection = new MagicCollection({
    value: totalPrice,
    total: totalCards,
    updated: new Date()
  });
  const res = await collection.save();
}

async function processBatch(records) {
  console.log(`Processing batch of ${records.length}`);

  const identifiers = [];
  const cards = [];

  for (let record of records) {
    identifiers.push({ id: record.scryfallId });
  }
  const query = {
    identifiers: identifiers
  };

  try {
    const res = await axios({
      method: 'post',
      url: SCRYFALL_COLLECTION_ENDPOINT,
      headers: {
        'User-Agent': USER_AGENT
      },
      data: query
    });

    for (let card of res.data.data) {
      const newCard = records.find(o => o.scryfallId === card.id);
      newCard.name = card.name;
      newCard.setCode = card.set;
      newCard.setName = card.set_name;
      newCard.rarity = card.rarity;
      newCard.layout = card.layout;
      totalPrice += newCard.price * newCard.quantity;
      if (card.image_uris) {
        newCard.images = {
          front: {
            small: card.image_uris.small,
            normal: card.image_uris.normal,
            large: card.image_uris.large
          }
        };
      } else {
        newCard.images = {
          front: {
            small: card.card_faces[0].image_uris.small,
            normal: card.card_faces[0].image_uris.normal,
            large: card.card_faces[0].image_uris.large
          }
        }
        if (card.card_faces.length > 1) {
          newCard.images.back = {
            small: card.card_faces[1].image_uris.small,
            normal: card.card_faces[1].image_uris.normal,
            large: card.card_faces[1].image_uris.large
          }
        }
      }
      cards.push(newCard);
    }

    await saveCards(cards);

    return records.length;
  } catch (err) {
    await exit(err);
  }
}

async function processRecords(records) {
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const n = await processBatch(batch);
    totalCards += n;
    await new Promise(resolve => setTimeout(resolve, SCRYFALL_TIME_LIMIT));
  }
  console.log(`Processed ${totalCards} cards total value ${formatCurrency(totalPrice/100)}`);

  await saveCollection();
}

async function connectToDb() {
  db = await mongoose.connect(MONGODB_URI);

  MagicCollectionSchema = new mongoose.Schema({
    total: {
      type: 'Number',
      required: true
    },
    value: {
      type: 'Number',
      required: true
    },
    updated: {
      type: 'Date',
      required: true
    },
  });
  MagicCollection = mongoose.model('MagicCollection', MagicCollectionSchema, 'magicCollection');

  MagicCardSchema = new mongoose.Schema({
    name: {
      type: 'String',
      required: true
    },
    setCode: {
      type: 'String',
      required: true
    },
    setName: {
      type: 'String',
      required: true
    },
    quantity: {
      type: 'Number',
      required: true
    },
    price: {
      type: 'Number',
      required: true
    },
    rarity: {
      type: 'String',
      required: true
    },
    foil: {
      type: 'Boolean',
      required: true
    },
    manaboxId: {
      type: 'String',
      required: true
    },
    scryfallId: {
      type: 'String',
      required: true
    },
    images: {
      front: {
        small: {
          type: 'String',
          required: true
        },
        normal: {
          type: 'String',
          required: true
        },
        large: {
          type: 'String',
          required: true
        }
      },
      back: {
        small: {
          type: 'String'
        },
        normal: {
          type: 'String'
        },
        large: {
          type: 'String'
        }
      }
    }
  });
  MagicCard = mongoose.model('MagicCard', MagicCardSchema, 'magicCards');

  let res = await MagicCollection.deleteMany({});
  res = await MagicCard.deleteMany({});
}

(async () => {
  console.log('Importing manabox data to MongoDB');

  try {
    await connectToDb();

    const records = await readCsv();
    await processRecords(records);

    if (db) {
      await db.disconnect();
    }
  } catch (err) {
    await exit(err);
  }
})();
