-- Create a copy of the raw data

SELECT *
FROM world_layoffs.layoffs
;

CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs
;

INSERT layoffs_staging
SELECT *
FROM layoffs
;

SELECT * FROM world_layoffs.layoffs_staging;

-- Let's start clearning the dataset
-- 1. Remove duplicate data
-- Ussually I remove the duplicate data by using the colume with dataId such as CustomerId or OrderId,etc. In this dataset, there is no Id column, so I would set the row_number for each row OVER PARTITION BY to figure out if is there any row have the same value with it

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, `date`) AS row_num
FROM layoffs_staging
;

WITH CTE_duplicate_check AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM CTE_duplicate_check
WHERE row_num >= 2
;

-- There are some rows duplicate including Casper, Cazoo, Hibob, Wildlife Studios, Yahoo
-- Let's review each company

SELECT *
FROM layoffs_staging
WHERE company = 'Casper'
;

-- Create a new table to delete duplicate rows. The table should be the same with the CTE table above which includes row_number
-- Go to layoffs_staging, right click, choose copy clipboard, then create table, the paste. To create the same CTE table, need to add a column call row_num

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
SELECT *
FROM layoffs_staging2
;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

DELETE
FROM layoffs_staging2
WHERE row_num > 1
;

-- 2. Standardizing data
-- Look at each column

SELECT company
FROM layoffs_staging2
;

SELECT company, TRIM(company)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)
;

-- industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
WHERE industry like 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE  industry like 'Crypto%'
;

-- location

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1
;

-- country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;

-- we see there are 2 rows of United States with different format. Let's fix it

SELECT DISTINCT country , TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

-- 3. Change data type
-- change the type of column date

SELECT `date`,
STR_TO_DATE (`date`,'%m/%d/%Y')
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y')
;

SELECT *
FROM layoffs_staging2;

-- Convert date type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 4. Remove Null or Blank Value

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
;
-- I find 4 rows missing industry  including companies Airbnb, Bally's, Carvana, Juul
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
;

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry is NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
; 

UPDATE layoffs_staging2
SET industry = null
WHERE industry = ''
;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company

SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%'
;

-- Look at total_laid_off

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- There are a lot of rows with total_laid_off and percentage_laid_off ARE NULLayoffs_staging2
-- Consider if keep these columns needed or should be deleted

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Let's have a final look of the final dataset
SELECT *
FROM layoffs_staging2
;