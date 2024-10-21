# DATA CLEANING PROJECT
# World layoff per company since 2021
# https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT *
FROM layoffs;

# Create duplicate table of raw data

CREATE TABLE layoff_stagging
LIKE layoffs;

SELECT *
FROM layoff_stagging;

INSERT layoff_stagging
SELECT *
FROM layoffs;

# 1. Identify duplicates

SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoff_stagging;

WITH duplicate_cte AS 
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry,total_laid_off,percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_stagging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoff_stagging
WHERE company = 'Casper';


# 2. Removing duplicates 

CREATE TABLE `layoff_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoff_stagging2; 

INSERT INTO layoff_stagging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoff_stagging;

SELECT *
FROM layoff_stagging2
WHERE row_num > 1; 

SET SQL_SAFE_UPDATES = 0;

DELETE
FROM layoff_stagging2
WHERE row_num > 1;

# 3. Standardizing data

 SELECT company, TRIM(company)
 FROM layoff_stagging2;
 
 UPDATE layoff_stagging2
 SET company = TRIM(company);
 
 SELECT DISTINCT industry
 FROM layoff_stagging2
 ORDER BY 1;

SELECT *
FROM layoff_stagging2
WHERE industry LIKE 'Crypto%';

UPDATE layoff_stagging2
SET industry =  'Crypto'
WHERE industry LIKE 'Crypto%';	

 SELECT DISTINCT industry
 FROM layoff_stagging2;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoff_stagging2
ORDER BY 1;
 
UPDATE layoff_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

# Changing date from text to integer

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoff_stagging2; 

UPDATE layoff_stagging2
SET `date` = 
    CASE
        WHEN `date` IS NOT NULL AND `date` != 'NULL' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
        ELSE NULL  
    END;
    
SELECT `date`
FROM layoff_stagging2;

ALTER TABLE layoff_stagging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoff_stagging2;


