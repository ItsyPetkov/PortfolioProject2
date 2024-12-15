IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'server_config')
BEGIN
	EXEC('
		CREATE PROCEDURE server_config
		AS
	
		EXEC sp_configure ''show advanced options'', 1;
		RECONFIGURE;
		EXEC sp_configure;

		EXEC sp_configure ''ad hoc distributed queries'', 1;
		RECONFIGURE;
		EXEC sp_configure;
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'database_config')
BEGIN
	EXEC('
		CREATE PROCEDURE database_config
		AS

		EXEC master.dbo.sp_MSset_oledb_prop N''Microsoft.ACE.OLEDB.12.0'', N''AllowInProcess'', 1 

		EXEC master.dbo.sp_MSset_oledb_prop N''Microsoft.ACE.OLEDB.12.0'', N''DynamicParameters'', 1 
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'populate_database')
BEGIN
	EXEC('
		CREATE PROCEDURE populate_database
		AS

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''salaries'')
		BEGIN
		CREATE TABLE salaries(
			comp_code VARCHAR(MAX),
			comp_name VARCHAR(MAX),
			employee_id VARCHAR(MAX),
			employee_name VARCHAR(MAX),
			date VARCHAR(MAX),
			func_code VARCHAR(MAX),
			func VARCHAR(MAX),
			salary VARCHAR(MAX)
		)

		BULK INSERT salaries FROM ''C:\Users\Hristo\Downloads\archive(1)\salaries.csv''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '';'',
				ROWTERMINATOR = ''\n''
			);
		END;

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''functions'')
		BEGIN
		CREATE TABLE functions(
			function_code INT PRIMARY KEY,
			function_name VARCHAR(255),
			function_group VARCHAR(255)
		)

		BULK INSERT functions FROM ''C:\Users\Hristo\Downloads\archive(1)\functions.csv''
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = '';'',
				ROWTERMINATOR = ''\n''
			);
		END;

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''employees'')
		BEGIN
		CREATE TABLE employees(
			comp_code_emp VARCHAR(255),
			employee_code_emp INT,
			employee_name_emp VARCHAR(255),
			GEN_M_F VARCHAR(255),
			age INT
		)

		BULK INSERT employees FROM ''C:\Users\Hristo\Downloads\archive(1)\employees.csv''
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = '';'',
				ROWTERMINATOR = ''\n''
			);
		END;

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''companies'')
		BEGIN
		CREATE TABLE companies(
			company_name VARCHAR(255),
			company_city VARCHAR(255),
			company_state VARCHAR(255),
			company_type VARCHAR(255),
			const_site_category VARCHAR(255)
		)

		BULK INSERT companies FROM ''C:\Users\Hristo\Downloads\archive(1)\companies.csv''
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = '';'',
				ROWTERMINATOR = ''\n''
			);
		END;
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'merge_datasets')
BEGIN
	EXEC('
		CREATE PROCEDURE merge_datasets
		AS

		--Merging all the tables into a single dataset
		--Removing duplicate columns 
		--Renaming columns with bad column names
		--Organizing columns in a more consistent manner

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''emp_dataset'')
		BEGIN
		SELECT S.comp_code AS company_code, S.comp_name AS company_name, C.company_city, C.company_state, C.company_type, C.const_site_category AS company_construction_site_category,
		S.employee_id, S.employee_name, E.GEN_M_F AS gender, E.age, S.date, S.salary, S.func_code AS function_code, S.func AS function_name, F.function_group 
		INTO emp_dataset
		FROM salaries AS S
		LEFT JOIN companies AS C
		ON S.comp_name = C.company_name
		LEFT JOIN functions AS F
		ON S.func_code = F.function_code
		LEFT JOIN employees AS E
		ON S.employee_id = E.employee_code_emp
		END;
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'data_cleaning')
BEGIN
	EXEC('
		CREATE PROCEDURE data_cleaning
		AS

		--Applying ‘TRIM’ to remove all unwanted spaces from all text columns.
		--Checking for NULL values  
		--Deleting NULL values (p.s. proceed with CAUTION this generally bad practice in the real world, especcially without permission or discussion prior to doing it)
		--Populating NULL values (p.s. in this case nothing chages as there are no NULL values but still worth showing)
		--Checking for empty values (p.s. in this case there are none because we deleted the NULL(s) already and TRIMMED the data)
		--Deleting empty values
		--Checking for duplicate rows
		--Removing duplicate rows
		--Standardizing date format
		--Alter table by adding new columns for future feature engineering
		--Look at the data after all those changes



		UPDATE emp_dataset
		SET company_code = TRIM(company_code),
			company_name = TRIM(company_name),
			company_city = TRIM(company_city),
			company_state = TRIM(company_state),
			company_type = TRIM(company_type),
			company_construction_site_category = TRIM(company_construction_site_category),
			employee_id = TRIM(employee_id),
			employee_name = TRIM(employee_name),
			gender = TRIM(gender),
			age = CAST(TRIM(CAST(age AS VARCHAR(255))) AS INT),
			date = TRIM(date),
			salary = TRIM(salary),
			function_code = TRIM(function_code),
			function_name = TRIM(function_name),
			function_group = TRIM(function_group)



		SELECT *, COUNT(*) OVER() AS null_count
		FROM emp_dataset
		WHERE company_code IS NULL
			OR company_name IS NULL
			OR company_city IS NULL
			OR company_state IS NULL
			OR company_type IS NULL
			OR company_construction_site_category IS NULL
			OR employee_id IS NULL
			OR employee_name IS NULL
			OR gender IS NULL
			OR age IS NULL
			OR date IS NULL
			OR salary IS NULL
			OR function_code IS NULL
			OR function_name IS NULL
			OR function_group IS NULL



		DELETE FROM emp_dataset
		WHERE company_code IS NULL
			OR company_name IS NULL
			OR company_city IS NULL
			OR company_state IS NULL
			OR company_type IS NULL
			OR company_construction_site_category IS NULL
			OR employee_id IS NULL
			OR employee_name IS NULL
			OR gender IS NULL
			OR age IS NULL
			OR date IS NULL
			OR salary IS NULL
			OR function_code IS NULL
			OR function_name IS NULL
			OR function_group IS NULL



		UPDATE emp_dataset
		SET company_code = ISNULL(company_code, ''No data available''), --NOTE: this can work with real values if you have a reference point i.e. a value from  
		company_name = ISNULL(company_name, ''No data available''),		--another row you know for sure can populate the missing value in a particular row
		company_city = ISNULL(company_city, ''No data available''),     --requires an actual query to work with real data
		company_state = ISNULL(company_state, ''No data available''),
		company_type = ISNULL(company_type, ''No data available''),
		company_construction_site_category = ISNULL(company_construction_site_category, ''No data available''),
		employee_id = ISNULL(employee_id, ''No data available''),
		employee_name = ISNULL(employee_name, ''No data available''),
		gender = ISNULL(gender, ''No data available''),
		age = ISNULL(age, ''-1''),										--Note: -1 here works fine as noone can be of age -1 so we can consider -1 as missing value in this case
		date = ISNULL(date, ''No data available''),
		salary = ISNULL(salary, ''No data available''),
		function_code = ISNULL(function_code, ''No data available''),
		function_name = ISNULL(function_name, ''No data available''),
		function_group = ISNULL(function_group, ''No data available'')



		SELECT *, COUNT(*) OVER() AS empty_count
		FROM emp_dataset
		WHERE company_code = '' ''
			OR company_name = '' ''
			OR company_city = '' ''
			OR company_state = '' ''
			OR company_type = '' ''
			OR company_construction_site_category = '' ''
			OR employee_id = '' ''
			OR employee_name = '' ''
			OR gender = '' ''
			OR age = '' ''
			OR date = '' ''
			OR salary = '' ''
			OR function_code = '' ''
			OR function_name = '' ''
			OR function_group = '' ''



		DELETE FROM emp_dataset
		WHERE company_code = '' ''
			OR company_name = '' ''
			OR company_city = '' ''
			OR company_state = '' ''
			OR company_type = '' ''
			OR company_construction_site_category = '' ''
			OR employee_id = '' ''
			OR employee_name = '' ''
			OR gender = '' ''
			OR age = '' ''
			OR date = '' ''
			OR salary = '' ''
			OR function_code = '' ''
			OR function_name = '' ''
			OR function_group = '' ''



		SELECT company_code, company_name, company_city, company_state, company_type, company_construction_site_category,
		employee_id, employee_name, gender, age, date, salary, function_code, function_name, function_group, COUNT(*) AS duplicate_count
		FROM emp_dataset
		GROUP BY company_code, company_name, company_city, company_state, company_type, company_construction_site_category,
		employee_id, employee_name, gender, age, date, salary, function_code, function_name, function_group
		HAVING COUNT(*) > 1;



		WITH remove_duplicates AS(
			SELECT company_code, company_name, company_city, company_state, company_type, company_construction_site_category,
			employee_id, employee_name, gender, age, date, salary, function_code, function_name, function_group, ROW_NUMBER() OVER(PARTITION BY 
			company_code, company_name, company_city, company_state, company_type, company_construction_site_category,
			employee_id, employee_name, gender, age, date, salary, function_code, function_name, function_group ORDER BY company_code) AS duplicate_count
			FROM emp_dataset
		)
		DELETE FROM remove_duplicates WHERE duplicate_count <> 1



		UPDATE emp_dataset
		SET date = CAST(date AS DATE)

		

		ALTER TABLE emp_dataset 
		ADD company_location VARCHAR(255), 
		employee_first_name VARCHAR(255),
		employee_last_name VARCHAR(255)



		SELECT TOP 500 * FROM emp_dataset
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'feature_engineering')
BEGIN
	EXEC('
		CREATE PROCEDURE feature_engineering
		AS

		--Feature engineering (split the entire employee name into first and last name)
		--Feature engineering (combine relevant company data to form its location)



		UPDATE emp_dataset
		SET company_location = company_city + '', '' + company_state;



		UPDATE emp_dataset
		SET employee_first_name = TRIM(SUBSTRING(employee_name, 1, CHARINDEX('' '', employee_name)))



		UPDATE emp_dataset
		SET employee_last_name = TRIM(SUBSTRING(employee_name, CHARINDEX('' '', employee_name), LEN(employee_name)))
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'sort_dataset')
BEGIN
	EXEC('
		CREATE PROCEDURE sort_dataset
		AS

		IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''final_emp_dataset'')
		BEGIN
		SELECT company_code, company_name, company_city, company_state, company_location, company_type, company_construction_site_category,
		employee_id, employee_name, employee_first_name, employee_last_name, gender, age, date, salary, function_code, function_name, function_group 
		INTO final_emp_dataset
		FROM emp_dataset
		END;
	')
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'backup_database')
BEGIN
	EXEC('
		CREATE PROCEDURE backup_database
		AS

		BACKUP DATABASE [ETLPortfolioProject] TO  DISK = N''C:\Program Files\Microsoft SQL Server\MSSQL16.SQLEXPRESS\MSSQL\Backup\finalDB.bak''
		WITH NOFORMAT, NOINIT,  NAME = N''ETLPortfolioProject-Full Database Backup'', SKIP, NOREWIND, NOUNLOAD,  STATS = 10
	')
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'extract_procedure')
BEGIN
	EXEC('
		CREATE PROCEDURE extract_procedure
		AS

		EXEC server_config
		EXEC database_config
		EXEC populate_database
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'transform_procedure')
BEGIN
	EXEC('
		CREATE PROCEDURE transform_procedure
		AS

		EXEC merge_datasets
		EXEC data_cleaning
		EXEC feature_engineering
	');
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'load_procedure')
BEGIN
	EXEC('
		CREATE PROCEDURE load_procedure
		AS

		EXEC sort_dataset
		EXEC backup_database
	')
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'ETL_procedure_pipeline')
BEGIN
	EXEC('
		CREATE PROCEDURE ETL_procedure_pipeline
		AS

		EXEC extract_procedure
		EXEC transform_procedure
		EXEC load_procedure
	');
END;

EXEC ETLPortfolioProject.dbo.ETL_procedure_pipeline