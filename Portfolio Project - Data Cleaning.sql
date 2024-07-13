-- SQL Project - Data Cleaning Project 


-- https:// www.kaggle.com/datasets/swaptr/layoffs - 2022


-- Sonwabo Nkosi 





SELECT *
FROM layoffs;


-- Create a staging table in order to work and  clean the data and stil have a table with raw data in case something happenes.


CREATE TABLE layoffs_staging
LIKE layoffs;


SELECT *
FROM layoffs_staging;


INSERT layoffs_staging
SELECT *
FROM layoffs;


-- Steps to follow when cleaning the data.
-- STEP:
-- 1. Remove Duplicates 
-- 2. Standardize the Data 
-- 3. Null Values or Blank Values 
-- 4. Remove Any Coloumns or rows



-- 1. Remove Duplicates
-- Check for Duplicates 


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplcate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplcate_cte
WHERE row_num > 1;



DROP TABLE IF EXISTS layoffs_staging2;
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


SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;


SELECT *
FROM layoffs_staging2
WHERE row_num > 1;



-- 2. Standardize the Data 


SELECT DISTINCT(company)
FROM layoffs_staging2;


SELECT company, TRIM(company)
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;


SELECT Distinct industry
FROM layoffs_staging2
ORDER BY 1;


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT industry
FROM layoffs_staging2
Order BY 1;


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;


SHOW COLUMNS 
FROM layoffs_staging2;


SELECT `date`,STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


SELECT `date`
from layoffs_staging2;


SHOW COLUMNS 
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
MODIFY COLUMN  `date` DATE;


SHOW COLUMNS 
FROM layoffs_staging2;



-- 3. Null Values or Blank Value


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';


SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT industry
FROM layoffs_staging2
WHERE industry IS NULL;


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';


SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


-- 4. Remove Any Coloumns 


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS null;


SELECT COUNT(*)
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS null;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS null;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS null;



SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT *
FROM layoffs_staging2;