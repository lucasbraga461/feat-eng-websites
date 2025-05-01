# 🚀 Creative Website Quality Scoring  
**Creative Feature-Engineering from Websites** using Python, Pyspark and Snowflake

---

## 🔍 Problem & Value  
Imagine you manage thousands of merchants across 6+ markets—each with its own website—and need to pick the best partners for a new proposal. Manually inspecting sites is impossible at scale. This repo automates a **Website Quality Score (0–10)** by measuring content depth, navigation, and product-price visibility. Integrate that score as a high-impact feature in your ML pipeline to rank and recommend top merchants with confidence.  

---

## 📦 Repository Layout  
```
feat-eng-websites/  
├── src/  
│   ├── helpers/  
│   │   └── snowflake_data_fetch.py    ← Snowflake I/O helper  
│   ├── p1_fetch_html_from_websites.py ← Async scraper (Snowflake or CSV)  
│   └── process_data/  
│       ├── s1_gather_initial_table.sql  
│       └── s2_create_table_with_website_feature.sql  
├── notebooks/  
│   └── ps_website_quality_score.ipynb  ← Snowpark feature extraction  
├── data/  
│   └── websites_initial_table.csv     ← Sample CSV input  
├── requirements.txt  
├── README.md  
└── .env, .gitignore, venv/  
```  

---

## ⚙️ Installation  
```bash
git clone https://github.com/lucasbraga461/feat-eng-websites.git  
cd feat-eng-websites  
python3 -m venv venv && source venv/bin/activate  
pip install -r requirements.txt  
```  

---

## 🔧 Configuration  
Open `src/p1_fetch_html_from_websites.py`, set:  
- `country_code` (e.g. `BRA`, `ARG`, `COL`, `MEX`, `CAN`, `JAM`)  
- Snowflake vs CSV mode via `--use_snowflake` flag  

Define each market’s language rules in `country_configs` (contact/about keywords + price regexes).

---

## 💾 Step 1: Scrape HTML  
### Snowflake mode  
```bash
python src/p1_fetch_html_from_websites.py -c BRA --use_snowflake
```  
- Reads `WEBSITES_INITIAL_TABLE` from Snowflake  
- Writes `WEBSITE_SCRAPED_DATA_<COUNTRY>` back to Snowflake  

### CSV mode  
```bash
python src/p1_fetch_html_from_websites.py -c BRA
```  
- Reads `data/websites_initial_table.csv`  
- Writes `data/website_scraped_data_BRA.csv`  

---

## 🔄 Why this is Better than other basic methods  
| **Method**       | **Basic**                           | **This Script**                                                                              |
|------------------|-------------------------------------|----------------------------------------------------------------------------------------------|
| HTTP Calls       | `requests.get()` sequential         | `asyncio + aiohttp` for concurrent, non-blocking I/O                                        |
| User-Agent       | One default header                  | Rotates real browser UAs to avoid throttling                                                |
| Batching         | All URLs at once                    | Checkpointable chunks (`BATCH_SIZE`)                                                         |
| Retries/Timeout  | Library defaults or crash           | Configurable `MAX_RETRIES` & `TIMEOUT` to handle flaky hosts                                 |
| Concurrency      | Unbounded or one-by-one             | `CONCURRENT_REQUESTS` + `asyncio.Semaphore` + `TCPConnector` to cap in-flight connections     |
| Event Loop       | Single loop—binding errors on restart | Fresh loop per batch to avoid “bound to different loop” issues and ensure isolation           |

---

## ⚡ Step 2: Extract Features (Snowpark)  
1. **Launch** the notebook:  
   ```bash
   jupyter lab notebooks/ps_website_quality_score.ipynb
   ```  
2. **Register** the Python UDF (`extract_features_udf`) in `@STAGE_WEBSITES`  
3. **Parse** raw HTML → word counts, title length, link/img/script counts, price flags  
4. **Compute** 0–10 quality score using business rules  
5. **Save** final table to Snowflake  

---

## 📊 Integrating Into ML  
Use `quality_score` alongside sales, reviews or demographics in your model. It quantifies online professionalism—boosting your classifier or regressor’s ability to spot top-tier merchants.

---

## 🤝 Contributing  
1. **Fork** & **clone**  
2. Create branch: `git checkout -b feature/XYZ`  
3. Commit & push:  
   ```bash
   git commit -am "Add feature XYZ"
   git push origin feature/XYZ
   ```  
4. Open a **Pull Request**

---

## 📜 License  
This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.  

---

## Author

Developed by **Lucas Braga**, Data Scientist  
[Connect on LinkedIn](https://www.linkedin.com/in/lucasbraga461/)

