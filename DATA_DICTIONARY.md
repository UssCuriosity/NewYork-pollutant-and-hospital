# 数据字典 — NYC 污染健康研究数据工程项目

> 版本：1.0 | 更新日期：2026-03  
> 项目：NYC PM2.5/NO2/SO2 对不同年龄阶段居民健康影响（2022–2024）

---

## 目录

1. [主输出文件：merged_for_GAM.csv](#1-主输出文件merged_for_gamcsv)
2. [月级备用文件：merged_for_GAM_monthly.csv](#2-月级备用文件merged_for_gam_monthlycsv)
3. [中间文件：data/raw/](#3-中间文件dataraw)
4. [数据源说明](#4-数据源说明)
5. [ICD-10 代码筛选逻辑](#5-icd-10-代码筛选逻辑)
6. [年龄分组标准化规则](#6-年龄分组标准化规则)
7. [EPA AQS 暴露度量方法](#7-epa-aqs-暴露度量方法)
8. [SPARCS PUF 时间粒度局限性](#8-sparcs-puf-时间粒度局限性)
9. [已知数据质量问题与处理](#9-已知数据质量问题与处理)
10. [建模使用指南](#10-建模使用指南)

---

## 1. 主输出文件：merged_for_GAM.csv

**位置：** `outputs/merged_for_GAM.csv`  
**维度：** 逐日 × 年龄组（每天 5 行，共约 5475 行 / 3 年）  
**编码：** UTF-8，逗号分隔

### 字段定义

| # | 字段名 | 类型 | 单位 | 说明 |
|---|--------|------|------|------|
| 1 | `date` | string | YYYY-MM-DD | 日历日期 |
| 2 | `age_group` | string | — | 年龄组（见第 6 节） |
| 3 | `n_inpatient` | integer | 病例数 | 当日住院就诊次数（ICD-10 筛选后，NYC 五县合计） |
| 4 | `n_ed` | integer | 病例数 | 当日急诊就诊次数（ICD-10 筛选后） |
| 5 | `n_outpatient` | integer | 病例数 | 当日门诊就诊次数（ICD-10 筛选后） |
| 6 | `pm25` | float | μg/m³ | 日均细颗粒物浓度（PM₂.₅，空气动力学直径 ≤2.5 μm） |
| 7 | `no2` | float | ppb | 日均二氧化氮浓度（体积比浓度，parts per billion） |
| 8 | `so2` | float | ppb | 日均二氧化硫浓度 |
| 9 | `dow` | integer | 0–6 | 星期几（0=周一，6=周日；ISO 8601 周一起始） |
| 10 | `month` | integer | 1–12 | 月份 |
| 11 | `year` | integer | — | 年份（2022 / 2023 / 2024） |
| 12 | `is_holiday` | integer | 0/1 | 是否美国联邦法定节假日（1=是） |
| 13 | `date_resolution` | string | — | 健康数据时间粒度标记（见第 8 节） |

### 缺失值约定

| 情形 | 处理方式 |
|------|---------|
| EPA 污染浓度缺失（≤3 天连续） | 线性插值填充 |
| EPA 污染浓度缺失（>3 天） | 保留 `NaN`，建模时须单独处理 |
| 健康结局无就诊记录 | 填充为 `0`（真实零计数） |
| 年龄分组无法映射 | `age_group="Unknown"`，建议建模前过滤 |

---

## 2. 月级备用文件：merged_for_GAM_monthly.csv

**位置：** `outputs/merged_for_GAM_monthly.csv`  
**维度：** 按月 × 年龄组聚合（每月 5 行，共约 180 行 / 3 年）

| 字段名 | 说明 |
|--------|------|
| `month_period` | 年月（格式 `YYYY-MM`，pandas Period 字符串） |
| `age_group` | 年龄组 |
| `n_inpatient` | 月内住院病例总数 |
| `n_ed` | 月内急诊病例总数 |
| `n_outpatient` | 月内门诊病例总数 |
| `pm25` | 月均 PM2.5 (μg/m³) |
| `no2` | 月均 NO2 (ppb) |
| `so2` | 月均 SO2 (ppb) |
| `holiday_days_in_month` | 该月内法定节假日天数 |

**适用场景：** 当 SPARCS 数据仅有月级粒度时（`date_resolution=monthly`），
应优先使用本文件进行 DLNM / 时间序列建模，以避免日均摊分布假设的偏差。

---

## 3. 中间文件：data/raw/

### EPA AQS 原始文件

**命名规则：** `epa_aqs_{param}_{county}_{year}.csv`（单县）
/ `epa_aqs_{param}_{year}.csv`（五县合并）

| 字段 | 来源 | 说明 |
|------|------|------|
| `date` | AQS `date_local` | 采样日期（监测站本地时间） |
| `county_fips` | 构造 | 5位 FIPS 代码（州+县，例如 `36061`） |
| `county_name` | 构造 | 县可读名（`bronx`/`kings`/`new_york`/`queens`/`richmond`） |
| `param` | 构造 | 污染物名称（`pm25`/`no2`/`so2`） |
| `arithmetic_mean` | AQS | 当日所有有效小时值的算术平均（各监测站独立） |
| `observation_count` | AQS | 当日有效观测小时数（用于五县加权平均） |
| `units_of_measure` | AQS | 浓度单位（PM2.5: `Micrograms/cubic meter (LC)`；NO2/SO2: `Parts per billion`） |
| `site_num` | AQS | 监测站编号（如有多个站点） |

### SPARCS 标准化文件

**命名规则：** `sparcs_{type}_{year}.csv`（type = inpatient / ed / outpatient）

| 字段 | 说明 |
|------|------|
| `date_col` | 日期字符串（YYYY-MM-DD）；月级/年级数据时为 NaT |
| `discharge_year` | 出院年份（整数） |
| `discharge_month` | 出院月份（整数）；年级数据时为 NA |
| `date_resolution` | 时间粒度（`daily` / `monthly` / `annual`） |
| `age_group` | 标准化年龄组（见第 6 节） |
| `diagnosis_code` | 主诊断 ICD-10-CM 代码（清洗后，去空格，大写） |
| `is_icd10_match` | 布尔值：主诊断是否命中白名单（True = 纳入分析） |
| `data_type` | 数据类别（`inpatient` / `ed` / `outpatient`） |
| `facility_county` | 机构所在县（NYC 五县之一） |

---

## 4. 数据源说明

### 4.1 EPA AQS（暴露数据）

| 属性 | 详情 |
|------|------|
| 数据提供方 | 美国环保署（US EPA）空气质量系统（AQS） |
| 访问地址 | https://aqs.epa.gov/data/api |
| 覆盖区域 | 纽约市五县（Bronx、Kings、New York、Queens、Richmond） |
| 时间范围 | 2022-01-01 — 2024-12-31 |
| 空间分辨率 | 县级（多站点聚合） |
| 时间分辨率 | 逐日 |
| 参数代码 | PM2.5 = 88101；NO2 = 42602；SO2 = 42401 |
| 参考方法 | PM2.5: FRM/FEM（联邦参考/等效方法）；NO2: 化学发光法；SO2: 荧光法 |
| 许可 | 公开数据，无版权限制 |
| 引用格式 | U.S. EPA Air Quality System (AQS). Daily Summary Data. Retrieved from https://aqs.epa.gov/data/api |

**监测网络说明：**
- NYC 各县可能有 1–5 个监测站
- `clean_merge.py` 使用 `observation_count` 加权平均，以减少站点稀少县份的偏差
- 部分参数（尤其 SO2）在某些县的站点覆盖有限，缺失天数较多

### 4.2 SPARCS PUF（健康结局数据）

| 属性 | 详情 |
|------|------|
| 数据提供方 | 纽约州卫生厅（NYSDOH）SPARCS（Statewide Planning and Research Cooperative System） |
| 访问地址 | https://health.data.ny.gov/ |
| 数据类型 | 住院（Inpatient）、急诊（Emergency Department）、门诊（Outpatient） |
| 覆盖区域 | 纽约市五县内持牌医疗机构 |
| 时间范围 | 2022–2024（以出院/就诊日期为准） |
| 去标识化 | 已按 HIPAA 最低必要原则去标识；地理信息最细至县级 |
| 计数单位 | 就诊次数（非唯一患者数，同一患者多次就诊各计一次） |
| ICD 版本 | ICD-10-CM（2022–2024 年均适用） |
| 许可 | 纽约州开放数据许可（NY Open Data Terms of Use） |
| 引用格式 | New York State Department of Health. SPARCS De-identified Data Files. health.data.ny.gov |

---

## 5. ICD-10 代码筛选逻辑

本项目关注大气污染相关的呼吸系统与心血管系统疾病终点，
筛选基于主诊断代码（Primary Diagnosis / Principal Diagnosis）前缀匹配。

### 5.1 呼吸系统终点（J 章，J00–J98）

| 代码段 | 疾病名称 | 与污染关联依据 |
|--------|---------|----------------|
| J00–J06 | 急性上呼吸道感染 | PM2.5 加重黏膜炎症，增加感染易感性 |
| J09–J18 | 流感与肺炎 | 空气污染削弱肺部清除机制 |
| J20–J22 | 急性支气管炎/细支气管炎 | NO2 直接刺激下呼吸道 |
| J40–J44 | 慢性支气管炎 / COPD | 长期暴露加速疾病进展 |
| J45–J46 | 哮喘（含哮喘持续状态） | PM2.5 诱发哮喘发作（核心终点） |
| J80–J81 | ARDS / 肺水肿 | 急性严重污染事件相关 |
| J96 | 呼吸衰竭 | 污染加重 COPD/哮喘患者病情 |

### 5.2 心血管系统终点（I 章，I10–I74）

| 代码段 | 疾病名称 | 与污染关联依据 |
|--------|---------|----------------|
| I10–I11 | 高血压（急性发作） | PM2.5 通过氧化应激升高血压 |
| I20–I25 | 缺血性心脏病（心绞痛/心梗） | PM2.5 促进动脉粥样硬化、血栓形成 |
| I26 | 肺栓塞 | 污染增加凝血活性 |
| I46–I50 | 心脏骤停 / 心律失常 / 心衰 | 自主神经功能受损 |
| I60–I64 | 脑卒中（出血性/缺血性） | PM2.5 增加卒中住院和死亡风险 |
| I70–I74 | 外周血管疾病 | 长期污染暴露相关 |

### 5.3 修改筛选范围

编辑 `config.yaml` 中的 `icd10_prefixes` 列表，重新运行 `clean_merge.py` 即可。

```yaml
icd10_prefixes:
  respiratory:
    - "J45"   # 仅保留哮喘（缩窄研究范围示例）
  cardiovascular:
    - "I21"   # 仅保留急性心梗
```

---

## 6. 年龄分组标准化规则

SPARCS PUF 原始年龄字段在不同年份/版本间存在格式差异，
`clean_merge.py` 按以下规则统一映射：

| 标准分组 | 包含原始值（示例） | 人群流行病学意义 |
|---------|-----------------|----------------|
| `0-17` | "0 to 17", "Under 1", "1 to 4", "5 to 14", "15 to 17" | 儿童及青少年（发育中肺部，对污染敏感） |
| `18-44` | "18 to 29", "30 to 44", "18 to 24", "25 to 34", "35 to 44" | 青壮年（基线健康较好） |
| `45-64` | "45 to 59", "60 to 74", "45 to 54", "55 to 64" | 中年（慢性病发病率上升） |
| `65-74` | "65 to 74", "60 to 74"（部分重叠） | 老年（高危，心肺储备下降） |
| `75+` | "75 or Older", "75 to 84", "85 and over", "85 or Older" | 高龄老年（最脆弱群体） |

> **注意：** 原始字段 "60 to 74" 在不同数据集版本中可能被映射到 `45-64` 或 `65-74`，
> 取决于数据集实际切分方式。`config.yaml` 中的 `age_group_map` 可按需调整。

无法映射的年龄值将被标记为 `"Unknown"`，建议在建模前过滤。

---

## 7. EPA AQS 暴露度量方法

### 7.1 空间聚合

本研究以 **NYC 全市平均浓度**代表人群暴露：

```
NYC日均浓度 = Σ(站点日均值 × 站点observation_count) / Σ(observation_count)
```

- 权重 = 各站点当日有效观测小时数（`observation_count`）
- 聚合单位：跨五县所有站点

**局限性：**  
县级或更细空间分辨率的暴露差异未予体现。如需精细分析，
可使用 `epa_aqs_{param}_{county}_{year}.csv` 进行县级分层建模。

### 7.2 缺失值填补

| 缺口长度 | 处理方法 |
|---------|---------|
| 1–3 天 | 线性插值（前后插值，`limit_direction=both`） |
| >3 天连续 | 保留 `NaN`，在建模时单独处理（如多重插补或排除） |

**建议：** 在报告中说明各污染物的缺失天数比例。

### 7.3 浓度单位

| 污染物 | AQS 标准单位 | 本项目输出单位 |
|--------|------------|--------------|
| PM2.5 | Micrograms/cubic meter (LC) | μg/m³ |
| NO2 | Parts per billion | ppb |
| SO2 | Parts per billion | ppb |

> LC = 本地条件（Local Conditions）下的体积，非标准温压条件。
> 若与其他研究比较，注意确认单位一致性。

---

## 8. SPARCS PUF 时间粒度局限性

### 背景

SPARCS PUF（公开使用文件）系纽约州卫生厅对原始 SPARCS 数据去标识化处理后发布的版本。
为保护患者隐私，**部分年份/版本的 PUF 仅保留年份或年+月信息，而非精确的出院日期**。

### 粒度类型与影响

| `date_resolution` 值 | 含义 | 对日级分析的影响 | 建议处理 |
|---------------------|------|-----------------|---------|
| `daily` | 数据含逐日日期，直接使用 | 无偏差 | 直接建模 |
| `monthly` | 仅有年+月，已均摊到每日 | 引入均匀分布假设，低估日内变异 | 优先使用月级输出 |
| `annual` | 仅有年份 | 不适合日级分析 | 仅用 `merged_for_GAM_monthly.csv` |

### 推荐分析策略

1. **若 date_resolution 全为 daily**：直接使用 `merged_for_GAM.csv` 进行日级 GAM/DLNM
2. **若 date_resolution 含 monthly**：
   - 日级分析：在模型中加入 `date_resolution` 哑变量，注明数据质量限制
   - 月级分析：改用 `merged_for_GAM_monthly.csv`（推荐）
3. **在论文方法部分**：明确说明 SPARCS PUF 时间粒度及其对滞后效应估计的潜在影响

### 向纽约州申请精细数据

如研究需要精确到逐日出院日期，可申请 SPARCS 研究人员数据（非 PUF）：
https://www.health.ny.gov/statistics/sparcs/access/

---

## 9. 已知数据质量问题与处理

| 问题 | 影响字段 | 处理方式 |
|------|---------|---------|
| SO2 在部分县无监测站 | `so2` | 缺失天保留 NaN（插值仅填短期缺口） |
| SPARCS 2024 数据可能尚未发布 | `n_*` | 数据集 ID 配置为空时跳过，输出为 0 |
| ICD-10 代码格式含点（如 J45.9） | `diagnosis_code` | 脚本已去除点号，统一前缀匹配 |
| 年龄字段格式不统一 | `age_group` | 精确映射 + 模糊数字映射双保险 |
| 多站点重复日期 | `pm25/no2/so2` | 加权平均聚合，不重复计数 |
| 跨县就医（外县患者在 NYC 就医） | `n_*` | SPARCS 以机构县为准，符合流行病学惯例 |

---

## 10. 建模使用指南

### 10.1 GAM（广义加性模型）

```r
library(mgcv)
df <- read.csv("outputs/merged_for_GAM.csv")
df$date <- as.Date(df$date)

# 单污染物单年龄组示例（哮喘入院）
df_child <- subset(df, age_group == "0-17")
mod <- gam(
  n_inpatient ~ s(pm25, k=6) + s(as.numeric(date), k=100) +
                factor(dow) + is_holiday,
  data    = df_child,
  family  = nb(),        # 负二项（计数数据过离散）
  method  = "REML"
)
summary(mod)
```

### 10.2 DLNM（分布滞后非线性模型）

```r
library(dlnm)
# 构建 PM2.5 滞后矩阵（滞后 0–7 天）
cb_pm25 <- crossbasis(
  df$pm25,
  lag    = 7,
  argvar = list(fun="ns", df=4),
  arglag = list(fun="ns", df=3)
)
mod_dlnm <- glm(
  n_ed ~ cb_pm25 + ns(as.numeric(date), df=8*3) +
         factor(dow) + is_holiday,
  data   = df,
  family = quasipoisson()
)
pred <- crosspred(cb_pm25, mod_dlnm, at=seq(0,60,by=5), cumul=TRUE)
plot(pred, "overall")
```

### 10.3 注意事项

- **时间趋势控制**：使用自然样条（`ns`）或惩罚样条（`s`）控制季节性和长期趋势
- **星期效应**：`dow` 必须纳入模型（就诊模式有显著星期效应）
- **过离散**：健康计数数据通常需使用负二项或准泊松族
- **多污染物模型**：单独分析各污染物（共线性问题），或使用缩减秩模型
- **年龄分层**：建议按年龄组分别建模，最后汇总年龄特异性效应估计
- **date_resolution 标注**：论文中须注明健康数据时间粒度及其局限性
