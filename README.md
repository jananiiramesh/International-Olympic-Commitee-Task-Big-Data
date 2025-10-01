# IOC Centennial Data Analysis 🏅

This project was developed to support the **International Olympic Committee (IOC)** in preparing for their centennial celebration.  
The objective is to **recognize outstanding athletes and coaches** from the last decade (Olympics 2012, 2016, 2020) using the provided datasets.

---

## 📂 Dataset Overview

You are provided with CSV files organized into two folders:

- **Small Dataset** → for understanding the problem and validating correctness.
- **Large Dataset** → for performance testing (time and space complexity).

### Files
- `athletes_2012.csv`, `athletes_2016.csv`, `athletes_2020.csv`  
  Columns: `id, name, dob, height(m), weight(kg), sport, event, country, num_followers, num_articles, personal_best, coach_id`

- `coaches.csv`  
  Columns: `id, name, age, years_of_experience, sport, country, contract_id, certification_committee`

- `medals.csv`  
  Columns: `id, sport, event, year, country, medal`

---

## 🎯 Tasks

### Task 1.1: All-Time Best Athletes
Identify the **best athlete per sport** across 2012, 2016, and 2020.  
Ranking is based on **points scored from medals**:

| Year | Gold | Silver | Bronze |
|------|------|--------|--------|
| 2012 | 20   | 15     | 10     |
| 2016 | 12   | 8      | 6      |
| 2020 | 15   | 12     | 7      |

**Tie-breaking rules** (in order):
1. Higher total points  
2. More Gold medals  
3. More Silver medals  
4. More Bronze medals  
5. Lexicographically smallest name (uppercase)

---

### Task 1.2: Top International Coaches
Identify the **top 5 coaches** who trained athletes from **China, India, USA**.  
Ranking is based on the **total aggregated points of their athletes** (using the same scoring as above).

**Constraints:**
- A coach trains **one country per year** but can switch countries between years.
- Athlete’s sport must match the coach’s sport.
- Only years **2012, 2016, 2020** are valid.

**Tie-breaking rules** (same as for athletes).

---

## ⚙️ Execution Command

Run the PySpark script as:

```bash
python task1.py athletes_2012.csv athletes_2016.csv athletes_2020.csv coaches.csv medals.csv output.txt
