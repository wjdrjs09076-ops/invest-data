// scripts/fetch_sp500_tickers.mjs
import fs from "node:fs/promises";

const URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies";

async function main() {
  const res = await fetch(URL);
  const html = await res.text();

  // 심플하게 ticker 추출 (첫 번째 table에서)
  const matches = [...html.matchAll(/<td><a[^>]*>([A-Z.\-]+)<\/a><\/td>/g)];

  const tickers = matches.map((m) => m[1])
    .filter((t) => /^[A-Z.\-]+$/.test(t))
    .map((t) => t.replace(".", "-")); // BRK.B → BRK-B 변환

  const unique = [...new Set(tickers)];

  await fs.writeFile(
    "data/sp500_tickers.json",
    JSON.stringify(unique, null, 2)
  );

  console.log(`Saved ${unique.length} tickers.`);
}

main();
