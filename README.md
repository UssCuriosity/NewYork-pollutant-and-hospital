# NYC PM2.5 / NO2 / SO2 健康影响研究 — 数据工程项目

> 研究主题：纽约市（NYC）大气污染物（PM2.5、NO2、SO2）对不同年龄阶段居民
> 急诊 / 住院 / 门诊健康影响（2022–2024 年）  
> 输出：`outputs/merged_for_GAM.csv` — 逐日 × 年龄组 维度整合表，可直接供
> GAM / DLNM / TSCC 统计建模使用。

---

## 目录

- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [环境配置](#环境配置)
- [数据源与 API 配置](#数据源与-api-配置)
- [运行流程](#运行流程)
- [本地 CSV 手动替代方案](#本地-csv-手动替代方案)
- [输出说明](#输出说明)
- [SPARCS 时间粒度局限性说明](#sparcs-时间粒度局限性说明)
- [常见问题](#常见问题)

---

## 项目结构

```
nyc_pollution_health_2022_2024/
├── config.yaml                  ← 所有 API 密钥、数据集 ID、参数配置
├── requirements.txt
├── README.md                    ← 本文件
├── DATA_DICTIONARY.md           ← 字段定义与数据源说明
├── data/
│   ├── raw/                     ← fetch_*.py 输出的原始 CSV
│   └── processed/               ← clean_merge.py 中间产物
├── src/
│   ├── fetch_epa_aqs.py         ← EPA AQS 污染数据抓取
│   ├── fetch_sparcs_inpatient.py← SPARCS 住院数据抓取
│   ├── fetch_sparcs_ed.py       ← SPARCS 急诊数据抓取
│   ├── fetch_sparcs_outpatient.py← SPARCS 门诊数据抓取
│   ├── _sparcs_base.py          ← SPARCS 公共基础逻辑（内部模块）
│   └── clean_merge.py           ← 清洗、整合、输出
└── outputs/
    ├── merged_for_GAM.csv       ← 主输出（运行后生成）
    ├── merged_for_GAM_monthly.csv← 月级备用（运行后生成）
    └── pipeline.log             ← 运行日志
```

---

## 快速开始

```bash
# 1. 进入项目目录
cd nyc_pollution_health_2022_2024

# 2. 创建并激活虚拟环境（推荐）
python -m venv .venv
# Windows PowerShell：
.venv\Scripts\Activate.ps1
# macOS/Linux：
source .venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt

# 4. 填写 API 凭据（见"数据源与 API 配置"一节）
# 编辑 config.yaml，填入 epa_aqs.email / epa_aqs.key
# 以及 socrata.app_token（可选）

# 5. 抓取 EPA AQS 污染数据
python src/fetch_epa_aqs.py

# 6. 抓取 SPARCS 健康数据（三类）
python src/fetch_sparcs_inpatient.py
python src/fetch_sparcs_ed.py
python src/fetch_sparcs_outpatient.py

# 7. 清洗、整合，生成建模输入
python src/clean_merge.py
```

运行结束后，`outputs/merged_for_GAM.csv` 即为建模就绪数据文件。

---

## 环境配置

| 要求 | 版本 |
|------|------|
| Python | >= 3.10 |
| pandas | >= 2.2 |
| numpy | >= 1.26 |
| requests | >= 2.32 |
| sodapy | >= 2.2 |
| pyyaml | >= 6.0 |
| holidays | >= 0.46 |

所有依赖见 `requirements.txt`，使用 `pip install -r requirements.txt` 安装。

### 运行脚本时的工作目录

所有脚本均使用相对路径（相对于 `nyc_pollution_health_2022_2024/` 根目录），
**必须在 `src/` 目录内运行**，或按以下方式指定：

```bash
# 方式一：在 src/ 目录内运行
cd src
python fetch_epa_aqs.py

# 方式二：在项目根目录运行（亦可）
python src/fetch_epa_aqs.py
```

> 注意：由于 `_sparcs_base.py` 使用相对导入，`fetch_sparcs_*.py` 需要在
> `src/` 目录内运行，或将 `src/` 加入 `PYTHONPATH`：
> ```bash
> # Windows PowerShell
> $env:PYTHONPATH = "src"
> python src/fetch_sparcs_inpatient.py
> ```

---

## 数据源与 API 配置

### 1. EPA AQS API

**注册（免费）：**
1. 访问 [https://aqs.epa.gov/data/api/signup](https://aqs.epa.gov/data/api/signup)
2. 输入邮箱，系统自动发送 API Key

**填写 `config.yaml`：**
```yaml
epa_aqs:
  email: "your@email.com"
  key:   "your_api_key"
```

EPA AQS 脚本将自动拉取 NYC 五县（Bronx/Kings/New York/Queens/Richmond）
2022–2024 年的逐日 PM2.5、NO2、SO2 数据。

**API 文档：** https://aqs.epa.gov/aqsweb/documents/data_api.html

### 2. SPARCS PUF（纽约州健康数据）

**数据门户：** https://health.data.ny.gov/

**获取 App Token（推荐）：**
1. 注册账号：https://data.ny.gov/signup
2. 在账户设置中创建 App Token
3. 填入 `config.yaml`：
   ```yaml
   socrata:
     app_token: "your_socrata_app_token"
   ```
   不填写时以匿名身份访问（限速更严，每小时约 1000 次请求）。

**数据集 ID：**  
`config.yaml` 中已预填已知数据集 ID（截至 2024 年）。
如数据集更新，可在门户搜索 "SPARCS Inpatient De-identified" 等关键词，
获取最新数据集 ID（格式如 `vn5v-hh5r`），更新至相应位置。

---

## 运行流程

```
fetch_epa_aqs.py
  └── 输出：data/raw/epa_aqs_{param}_{county}_{year}.csv
             data/raw/epa_aqs_{param}_{year}.csv（五县合并）

fetch_sparcs_inpatient.py / fetch_sparcs_ed.py / fetch_sparcs_outpatient.py
  └── 输出：data/raw/sparcs_{type}_{year}.csv

clean_merge.py
  ├── 读取：data/raw/epa_aqs_*.csv + data/raw/sparcs_*.csv
  ├── 输出：outputs/merged_for_GAM.csv         （日级主表）
  │         outputs/merged_for_GAM_monthly.csv  （月级备用）
  │         data/processed/daily_merged_intermediate.csv
  └── 日志：outputs/pipeline.log
```

所有步骤均支持**缓存跳过**：若目标 CSV 已存在，脚本自动跳过 API 调用，
直接读取本地文件。强制重新拉取只需删除对应的 `data/raw/*.csv`。

---

## 本地 CSV 手动替代方案

若无 API 访问权限，可手动下载数据并放入 `data/raw/`：

### EPA AQS 手动下载
1. 访问 https://aqs.epa.gov/aqsweb/documents/data_api.html#daily
2. 选择"Download → Daily Summary Data → By County"
3. 设置参数：State=36，County=依次选 005/047/061/081/085，
   Parameter=88101（PM2.5）/42602（NO2）/42401（SO2）
4. 重命名文件：`epa_aqs_{param}_{county}_{year}.csv`
   - 参数名：`pm25` / `no2` / `so2`
   - 县名：`bronx` / `kings` / `new_york` / `queens` / `richmond`
   - 示例：`epa_aqs_pm25_bronx_2022.csv`

### SPARCS PUF 手动下载
1. 访问 https://health.data.ny.gov/
2. 搜索 "SPARCS Inpatient De-identified 2022"（及 2023/2024）
3. 导出为 CSV
4. 重命名为：`sparcs_inpatient_{year}.csv`、`sparcs_ed_{year}.csv`、
   `sparcs_outpatient_{year}.csv`，放入 `data/raw/`

---

## 输出说明

### `outputs/merged_for_GAM.csv`（主输出）

逐日 × 年龄组维度的整合表，共 `3年 × 365天 × 5年龄组 ≈ 5475 行`。

| 字段 | 说明 |
|------|------|
| `date` | 日期（YYYY-MM-DD） |
| `age_group` | 年龄组（0-17 / 18-44 / 45-64 / 65-74 / 75+） |
| `n_inpatient` | 当日住院病例数（ICD-10 筛选后） |
| `n_ed` | 当日急诊病例数 |
| `n_outpatient` | 当日门诊病例数 |
| `pm25` | 日均 PM2.5 (μg/m³)，NYC 五县站点加权平均 |
| `no2` | 日均 NO2 (ppb) |
| `so2` | 日均 SO2 (ppb) |
| `dow` | 星期几（0=周一，6=周日） |
| `month` | 月份（1–12） |
| `year` | 年份（2022/2023/2024） |
| `is_holiday` | 是否美国联邦节假日（0/1） |
| `date_resolution` | 健康数据时间粒度（daily/monthly/annual） |

完整字段说明见 `DATA_DICTIONARY.md`。

### `outputs/merged_for_GAM_monthly.csv`（月级备用）

按月 × 年龄组聚合，适用于月级 DLNM / 时间序列分析。

---

## SPARCS 时间粒度局限性说明

SPARCS PUF 公开数据的日期精度因版本而异：

| 数据集版本 | 日期字段 | 精度 | 处理策略 |
|-----------|---------|------|---------|
| 完整版（部分年份） | `discharge_date` | 逐日 | 直接使用，`date_resolution=daily` |
| 精简版 | `discharge_year` + `discharge_month` | 月级 | 月计数 ÷ 当月天数，`date_resolution=monthly` |
| 汇总版 | `discharge_year` 仅 | 年级 | 仅月级输出，`date_resolution=annual` |

**建模建议：**
- `date_resolution=daily`：可直接用于日级 GAM / DLNM
- `date_resolution=monthly`：建议使用 `merged_for_GAM_monthly.csv` 进行月级分析，
  或在模型中加入 `date_resolution` 作为协变量标注数据质量
- 所有结果报告中须注明 SPARCS PUF 时间粒度的局限性

---

## 常见问题

**Q: EPA AQS API 返回 "Invalid Key" 错误？**  
A: 新注册的 Key 需要等待约 15 分钟才能生效，请稍后重试。

**Q: Socrata 请求返回空数据？**  
A: 检查数据集 ID 是否正确（门户搜索最新 ID），或使用本地 CSV 回退方案。

**Q: `clean_merge.py` 提示 "No EPA data loaded"？**  
A: 确保先运行 `fetch_epa_aqs.py`，或手动放置 CSV 文件到 `data/raw/`。

**Q: 如何修改 ICD-10 筛选范围？**  
A: 编辑 `config.yaml` 中的 `icd10_prefixes` 列表，添加或删除代码前缀。

**Q: 如何扩展到其他污染物（如 O3）？**  
A: 在 `config.yaml` 的 `epa_aqs.parameters` 中添加新参数代码（O3=44201），
   `fetch_epa_aqs.py` 会自动处理，`clean_merge.py` 需相应添加合并逻辑。
