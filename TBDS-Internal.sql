USE [master]
GO
/****** Object:  Database [TBDS-Internal]    Script Date: 3/20/2017 2:15:19 PM ******/
CREATE DATABASE [TBDS-Internal]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'TBDS-Internal', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\TBDS-Internal.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'TBDS-Internal_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\TBDS-Internal_log.ldf' , SIZE = 1449984KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [TBDS-Internal] SET COMPATIBILITY_LEVEL = 130
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [TBDS-Internal].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [TBDS-Internal] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [TBDS-Internal] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [TBDS-Internal] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [TBDS-Internal] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [TBDS-Internal] SET ARITHABORT OFF 
GO
ALTER DATABASE [TBDS-Internal] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [TBDS-Internal] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [TBDS-Internal] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [TBDS-Internal] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [TBDS-Internal] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [TBDS-Internal] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [TBDS-Internal] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [TBDS-Internal] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [TBDS-Internal] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [TBDS-Internal] SET  DISABLE_BROKER 
GO
ALTER DATABASE [TBDS-Internal] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [TBDS-Internal] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [TBDS-Internal] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [TBDS-Internal] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [TBDS-Internal] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [TBDS-Internal] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [TBDS-Internal] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [TBDS-Internal] SET RECOVERY FULL 
GO
ALTER DATABASE [TBDS-Internal] SET  MULTI_USER 
GO
ALTER DATABASE [TBDS-Internal] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [TBDS-Internal] SET DB_CHAINING OFF 
GO
ALTER DATABASE [TBDS-Internal] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [TBDS-Internal] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [TBDS-Internal] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [TBDS-Internal] SET QUERY_STORE = OFF
GO
USE [TBDS-Internal]
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [TBDS-Internal]
GO
/****** Object:  User [TBDSAD\laurav]    Script Date: 3/20/2017 2:15:19 PM ******/
CREATE USER [TBDSAD\laurav] FOR LOGIN [TBDSAD\laurav] WITH DEFAULT_SCHEMA=[dbo]
GO
/****** Object:  User [TBDSAD\dbreader]    Script Date: 3/20/2017 2:15:19 PM ******/
CREATE USER [TBDSAD\dbreader] FOR LOGIN [TBDSAD\dbreader] WITH DEFAULT_SCHEMA=[dbo]
GO
/****** Object:  User [TBDS_PBI_Read]    Script Date: 3/20/2017 2:15:19 PM ******/
CREATE USER [TBDS_PBI_Read] FOR LOGIN [TBDS_PBI_Read] WITH DEFAULT_SCHEMA=[dbo]
GO
ALTER ROLE [db_datareader] ADD MEMBER [TBDSAD\laurav]
GO
ALTER ROLE [db_datareader] ADD MEMBER [TBDSAD\dbreader]
GO
ALTER ROLE [db_datareader] ADD MEMBER [TBDS_PBI_Read]
GO
/****** Object:  UserDefinedFunction [dbo].[fnGetMonthlyWorkDays]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		DeLange, Derek
-- Create date: 2017-02-23
-- Description:	Calculates the number of work days in a given month/year
-- =============================================
CREATE FUNCTION [dbo].[fnGetMonthlyWorkDays] 
(
	@year int,
	@month int
)
RETURNS int
AS
BEGIN
	IF NOT ISNULL(@Month, -1) BETWEEN 1 and 12
	RETURN -1

	IF @Year < 2000 
	RETURN -1

	DECLARE @StartDate DATETIME
	DECLARE @EndDate DATETIME
	DECLARE @MonthlyWorkDays INT
	DECLARE @GoodFridayMonth INT = MONTH(DATEADD(day, -2, dbo.GetEasterSunday(@year)))
	SET @StartDate = CAST(@month as varchar(2)) + '/1/' + CAST(@year as varchar(4))
	SET @EndDate = DATEADD(day, -1, DATEADD(month, 1, @StartDate))

	SET @MonthlyWorkDays = 
	   (DATEDIFF(dd, @StartDate, @EndDate) + 1)
	  -(DATEDIFF(wk, @StartDate, @EndDate) * 2)
	  -(CASE WHEN DATENAME(dw, @StartDate) = 'Sunday' THEN 1 ELSE 0 END)
	  -(CASE WHEN DATENAME(dw, @EndDate) = 'Saturday' THEN 1 ELSE 0 END)

	SET @MonthlyWorkDays = @MonthlyWorkDays - 
		CASE
			WHEN DATENAME(month, @StartDate) = 'January' and DATENAME(dw, @StartDate) <> 'Saturday'
			THEN 1 --If NYD is on a Sat, it's counted in the previous month
			WHEN DATENAME(month, @StartDate) = ('March') and @GoodFridayMonth = 3
			THEN 1
			WHEN DATENAME(month, @StartDate) = ('April') and @GoodFridayMonth = 4
			THEN 1
			WHEN DATENAME(month, @StartDate) = ('May')
			THEN 1 --Mem. Day
			WHEN DATENAME(month, @StartDate) = ('July')
			THEN 1 --Ind. Day
			WHEN DATENAME(month, @StartDate) = ('September')
			THEN 1 --Labor Day
			WHEN DATENAME(month, @StartDate) = ('November')
			THEN 3 --Vet's Day, Thanksgiving, Black Friday
			WHEN DATENAME(month, @StartDate) = ('December') and DATENAME(dw, @EndDate) = 'Friday'
			THEN 4 --Christmas/Eve, NYE... and NYD is counted in Dec if on a Saturday (i.e. NYE is on a Friday)
			WHEN DATENAME(month, @StartDate) = ('December') and DATENAME(dw, @EndDate) <> 'Friday'
			THEN 3 --Christmas/Eve, NYE... but not NYD
			ELSE 0 --other months don't have holidays :'-(
		END

	RETURN @MonthlyWorkDays

END

GO
/****** Object:  UserDefinedFunction [dbo].[GetEasterSunday]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[GetEasterSunday] 
( @Y INT ) 
RETURNS SMALLDATETIME 
AS 
BEGIN 
    DECLARE     @EpactCalc INT,  
        @PaschalDaysCalc INT, 
        @NumOfDaysToSunday INT, 
        @EasterMonth INT, 
        @EasterDay INT 

    SET @EpactCalc = (24 + 19 * (@Y % 19)) % 30 
    SET @PaschalDaysCalc = @EpactCalc - (@EpactCalc / 28) 
    SET @NumOfDaysToSunday = @PaschalDaysCalc - ( 
        (@Y + @Y / 4 + @PaschalDaysCalc - 13) % 7 
    ) 

    SET @EasterMonth = 3 + (@NumOfDaysToSunday + 40) / 44 

    SET @EasterDay = @NumOfDaysToSunday + 28 - ( 
        31 * (@EasterMonth / 4) 
    ) 

    RETURN 
    ( 
        SELECT CONVERT 
        (  SMALLDATETIME, 
                 RTRIM(@Y)  
            + RIGHT('0'+RTRIM(@EasterMonth), 2)  
            + RIGHT('0'+RTRIM(@EasterDay), 2)  
    )) 
END 

GO
/****** Object:  Table [dbo].[tblHarvestTimeEntries]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblHarvestTimeEntries](
	[TaskEntryID] [varchar](25) NOT NULL,
	[UserID] [varchar](25) NULL,
	[TaskDate] [datetime] NULL,
	[ProjectID] [varchar](25) NULL,
	[TaskID] [varchar](25) NULL,
	[Project] [varchar](max) NULL,
	[TaskName] [varchar](max) NULL,
	[Client] [varchar](max) NULL,
	[Notes] [varchar](max) NULL,
	[TaskEntryHours] [real] NULL,
	[RecordDeleted] [bit] NULL,
	[DeletedDate] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[TaskEntryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
/****** Object:  Table [dbo].[tblHarvestUsers]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblHarvestUsers](
	[UserID] [varchar](25) NOT NULL,
	[FirstName] [varchar](200) NULL,
	[LastName] [varchar](200) NULL,
	[WeeklyCapacity] [int] NULL,
	[WantsNewsletter] [varchar](10) NULL,
	[UpdatedAt] [varchar](50) NULL,
	[CreatedAt] [varchar](50) NULL,
	[IsContractor] [varchar](10) NULL,
	[IsActive] [varchar](10) NULL,
	[Telephone] [varchar](25) NULL,
	[Email] [varchar](200) NULL,
	[DefaultHrlyRate] [real] NULL,
	[CostRate] [real] NULL,
	[IsAdmin] [varchar](10) NULL,
	[HasAccessToFutureProjects] [varchar](10) NULL,
	[Department] [varchar](200) NULL,
	[Timezone] [varchar](200) NULL,
PRIMARY KEY CLUSTERED 
(
	[UserID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[tblHarvestTasks]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblHarvestTasks](
	[TaskID] [varchar](50) NOT NULL,
	[TaskName] [varchar](255) NULL,
	[Billable] [varchar](10) NULL,
	[CreatedAt] [varchar](50) NULL,
	[UpdatedAt] [varchar](50) NULL,
	[IsDefault] [varchar](10) NULL,
	[DefaultHourlyRate] [real] NULL,
	[Deactivated] [varchar](10) NULL,
PRIMARY KEY CLUSTERED 
(
	[TaskID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[tblHarvestProjects]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblHarvestProjects](
	[ProjectID] [varchar](50) NOT NULL,
	[ClientID] [varchar](50) NULL,
	[ProjectName] [varchar](255) NULL,
	[ProjectCode] [varchar](255) NULL,
	[IsActive] [varchar](10) NULL,
	[Billable] [varchar](10) NULL,
	[BillBy] [varchar](255) NULL,
	[Budget] [varchar](255) NULL,
	[BudgetBy] [varchar](255) NULL,
	[NotifyOverBudget] [varchar](10) NULL,
	[OverBudgetNotifyPctage] [varchar](20) NULL,
	[OverBudgetNotifiedAt] [varchar](255) NULL,
	[ShowBudgetToAll] [varchar](10) NULL,
	[CreatedAt] [varchar](50) NULL,
	[UpdatedAt] [varchar](50) NULL,
	[StartsOn] [varchar](50) NULL,
	[EndsOn] [varchar](50) NULL,
	[Estimate] [varchar](255) NULL,
	[EstimateBy] [varchar](255) NULL,
	[HintEarliestRecordAt] [varchar](50) NULL,
	[HintLatestRecordAt] [varchar](50) NULL,
	[Notes] [varchar](max) NULL,
	[HourlyRate] [varchar](20) NULL,
	[CostBudget] [varchar](20) NULL,
	[CostBudgetInclExpense] [varchar](20) NULL,
PRIMARY KEY CLUSTERED 
(
	[ProjectID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
/****** Object:  Table [dbo].[tblHarvestClients]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblHarvestClients](
	[ClientID] [varchar](25) NOT NULL,
	[ClientName] [varchar](max) NULL,
	[Active] [varchar](10) NULL,
	[Currency] [varchar](255) NULL,
	[HighRiseID] [varchar](255) NULL,
	[UpdatedAt] [varchar](50) NULL,
	[CreatedAt] [varchar](50) NULL,
	[StatementKey] [varchar](255) NULL,
	[DefaultInvoiceKind] [varchar](255) NULL,
	[DefaultInvoiceTimeFrame] [varchar](255) NULL,
	[ClientAddress] [varchar](max) NULL,
	[CacheVersion] [varchar](255) NULL,
	[CurrencySymbol] [nvarchar](10) NULL,
	[Details] [varchar](max) NULL,
	[LastInvoiceKind] [varchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[ClientID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
/****** Object:  View [dbo].[vMonthlyHoursSummary]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[vMonthlyHoursSummary] as

select
	u.Lastname + ', ' + u.FirstName as Employee,
	cast(Year(te.TaskDate) as varchar(4)) + '-' + right('00' + cast(Month(te.TaskDate) as varchar(2)), 2) as [Task Month],
	sum(
	case
		when p.ClientID = '3069700' --TBDS
		then case
				when t.Billable = 'true'
				then cast(te.TaskEntryHours as decimal(10,2))
				else 0
			end
		else 0
	end) as [PayableTaskHours],
	sum(
	case
		when p.ClientID <> '3069700' --TBDS
		then case
				when t.Billable = 'true'
				then cast(te.TaskEntryHours as decimal(10,2))
				else 0
			end
		else 0
	end) as [BillableTaskHours]
from
	tblHarvestTimeEntries te
	left join
	tblHarvestUsers u
		on te.UserID = u.UserID
	left join
	tblHarvestTasks t
		on te.TaskID = t.TaskID
	left join
	tblHarvestProjects p
		on te.ProjectID = p.ProjectID
	left join
	tblHarvestClients c
		on p.ClientID = c.ClientID
where
	case
		when p.ClientID = '3069700' --TBDS
		then case
				when t.Billable = 'true'
				then 'Payable - Non-Billable'
				else 'Non-Payable'
			end
		when p.ClientID <> '3069700' --not TBDS
		then case
				when t.Billable = 'true'
				then 'Payable - Billable'
				else 'Non-Payable'
			end
		else 'Other'
	end <> 'Non-Payable'
group by
	u.Lastname + ', ' + u.FirstName,
	cast(Year(te.TaskDate) as varchar(4)) + '-' + right('00' + cast(Month(te.TaskDate) as varchar(2)), 2)


GO
/****** Object:  View [dbo].[vMonthlyHoursTargetSnapshot]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vMonthlyHoursTargetSnapshot] as
select top 100 percent 
	q.*,
	cast((q.PayableTaskHours / (q.PayableTaskHours + q.BillableTaskHours) * 100) as decimal(10,2)) as nonBillablePct,
	dbo.fnGetMonthlyWorkDays(cast(left(q.[Task Month], 4) as int), cast(right(q.[Task Month], 2) as int)) as [MonthlyWorkDays],
	cast(dbo.fnGetMonthlyWorkDays(cast(left(q.[Task Month], 4) as int), cast(right(q.[Task Month], 2) as int)) * 4.4 as decimal(10, 2)) as [MonthlyWorkHours],
	cast(((q.BillableTaskHours + q.PayableTaskHours) / cast(dbo.fnGetMonthlyWorkDays(cast(left(q.[Task Month], 4) as int), cast(right(q.[Task Month], 2) as int)) * 4.4 as decimal(10, 2))) * 100 as decimal(10,2)) as [PctMonthlyHourlyTarget]
from
	vMonthlyHoursSummary as q
where
	q.Employee in (
		'DeLange, Derek',
		'Atkinson, Travis',
		'Hagedorn, Josh',
		'Bowman, Sarah')
order by
	q.Employee,
	q.[Task Month]



GO
/****** Object:  Table [dbo].[tblEmployees]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblEmployees](
	[EmpID] [int] IDENTITY(1,1) NOT NULL,
	[First Name] [nvarchar](255) NULL,
	[Last Name] [nvarchar](255) NULL,
	[Pay Type] [nvarchar](255) NULL,
	[Pay Rate] [money] NULL,
	[Exempt] [bit] NULL
) ON [PRIMARY]

GO
/****** Object:  Table [dbo].[tblTBDSHolidays]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblTBDSHolidays](
	[HolidayDate] [datetime] NOT NULL,
	[HolidayObservedDate] [datetime] NULL,
	[HolidayName] [varchar](50) NULL,
PRIMARY KEY CLUSTERED 
(
	[HolidayDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
/****** Object:  StoredProcedure [dbo].[spHarvestClientLoad]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE PROCEDURE [dbo].[spHarvestClientLoad] AS
 /*
 Author:		Derek DeLange
 Date Created:	2017-02-10
 Description:	Processes and loads JSON client records
				downloaded via Python script.

 Change Log:
 Date			Changed By				Description
 ==================================================
 2017-02-10		DeLange, Derek			Created SP

 ==================================================
 */

DECLARE @json varchar(max) = (select bulkcolumn from openrowset(BULK 'C:\HarvestImport\clientData.json', SINGLE_CLOB) as j)

	--update existing values in clients table:
	UPDATE hc
	SET
		hc.ClientID = c.ClientID,
		hc.ClientName = c.ClientName,
		hc.Active = c.Active,
		hc.Currency = c.Currency,
		hc.HighRiseID = c.HighRiseID,
		hc.UpdatedAt = c.UpdatedAt,
		hc.CreatedAt = c.CreatedAt,
		hc.StatementKey = c.StatementKey,
		hc.DefaultInvoiceKind = c.DefaultInvoiceKind,
		hc.DefaultInvoiceTimeFrame = c.DefaultInvoiceTimeFrame,
		hc.ClientAddress = c.ClientAddress,
		hc.CacheVersion = c.CacheVersion,
		hc.CurrencySymbol = c.CurrencySymbol,
		hc.Details = c.Details,
		hc.LastInvoiceKind = c.LastInvoiceKind
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				ClientID				varchar(25)		'$.client.id',
				ClientName				varchar(max)	'$.client.name',
				Active					varchar(10)		'$.client.active',
				Currency				varchar(255)	'$.client.currency',
				HighRiseID				varchar(255)	'$.client.highrise_id',
				UpdatedAt				varchar(50)		'$.client.updated_at',
				CreatedAt				varchar(50)		'$.client.created_at',
				StatementKey			varchar(255)	'$.client.statement_key',
				DefaultInvoiceKind		varchar(255)	'$.client.default_invoice_kind',
				DefaultInvoiceTimeFrame	varchar(255)	'$.client.default_invoice_timeframe',
				ClientAddress			varchar(max)	'$.client.address',
				CacheVersion			varchar(255)	'$.client.cache_version',
				CurrencySymbol			nvarchar(10)	'$.client.currency_symbol',
				Details					varchar(max)	'$.client.details',
				LastInvoiceKind			varchar(255)	'$.client.last_invoice_kind'
			)) as c
		inner join tblHarvestClients as hc
			on hc.ClientID = c.ClientID

	--insert new values to clients table:
	INSERT INTO tblHarvestClients
	SELECT
		c.*
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				ClientID				varchar(25)		'$.client.id',
				ClientName				varchar(max)	'$.client.name',
				Active					varchar(10)		'$.client.active',
				Currency				varchar(255)	'$.client.currency',
				HighRiseID				varchar(255)	'$.client.highrise_id',
				UpdatedAt				varchar(50)		'$.client.updated_at',
				CreatedAt				varchar(50)		'$.client.created_at',
				StatementKey			varchar(255)	'$.client.statement_key',
				DefaultInvoiceKind		varchar(255)	'$.client.default_invoice_kind',
				DefaultInvoiceTimeFrame	varchar(255)	'$.client.default_invoice_timeframe',
				ClientAddress			varchar(max)	'$.client.address',
				CacheVersion			varchar(255)	'$.client.cache_version',
				CurrencySymbol			nvarchar(10)	'$.client.currency_symbol',
				Details					varchar(max)	'$.client.details',
				LastInvoiceKind			varchar(255)	'$.client.last_invoice_kind'
			)) as c
		left join tblHarvestClients as hc
			on hc.ClientID = c.ClientID
		WHERE
			hc.ClientID IS NULL



GO
/****** Object:  StoredProcedure [dbo].[spHarvestImportMaster]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[spHarvestImportMaster] as
/*
 Author:		Derek DeLange
 Date Created:	2017-02-10
 Description:	Runs all stored procs for Harvest import

 Change Log:
 Date			Changed By				Description
 ==================================================
 2017-02-10		DeLange, Derek			Created SP

 ==================================================
 */

 --Import users:
 exec spHarvestUserLoad

 --Import projects:
 exec spHarvestProjectLoad

 --Import tasks:
 exec spHarvestTaskLoad

 --Import clients:
 exec spHarvestClientLoad

 --Import time entries:
 exec spHarvestTimeEntryLoad

GO
/****** Object:  StoredProcedure [dbo].[spHarvestProjectLoad]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE PROCEDURE [dbo].[spHarvestProjectLoad] AS
 /*
 Author:		Derek DeLange
 Date Created:	2017-02-10
 Description:	Processes and loads JSON project records
				downloaded via Python script.

 Change Log:
 Date			Changed By				Description
 ==================================================
 2017-02-10		DeLange, Derek			Created SP

 ==================================================
 */

DECLARE @json varchar(max) = (select bulkcolumn from openrowset(BULK 'C:\HarvestImport\projectData.json', SINGLE_CLOB) as j)

	--update existing values in project table:
	UPDATE hp
	SET
		hp.ProjectID = p.ProjectID,
		hp.ClientID = p.ClientID,
		hp.ProjectName = p.ProjectName,
		hp.ProjectCode = p.ProjectCode,
		hp.IsActive = p.IsActive,
		hp.Billable = p.Billable,
		hp.BillBy = p.BillBy,
		hp.Budget = p.Budget,
		hp.BudgetBy = p.BudgetBy,
		hp.NotifyOverBudget = p.NotifyOverBudget,
		hp.OverBudgetNotifyPctage = p.OverBudgetNotifyPctage,
		hp.OverBudgetNotifiedAt = p.OverBudgetNotifiedAt,
		hp.ShowBudgetToAll = p.ShowBudgetToAll,
		hp.CreatedAt = p.CreatedAt,
		hp.UpdatedAt = p.UpdatedAt,
		hp.StartsOn = p.StartsOn,
		hp.EndsOn = p.EndsOn,
		hp.Estimate = p.Estimate,
		hp.EstimateBy = p.EstimateBy,
		hp.HintEarliestRecordAt = p.HintEarliestRecordAt,
		hp.HintLatestRecordAt = p.HintLatestRecordAt,
		hp.Notes = p.Notes,
		hp.HourlyRate = p.HourlyRate,
		hp.CostBudget = p.CostBudget,
		hp.CostBudgetInclExpense = p.CostBudgetInclExpense
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				ProjectID		varchar(50)		'$.project.id',
				ClientID		varchar(50)		'$.project.client_id',
				ProjectName		varchar(255)	'$.project.name',
				ProjectCode		varchar(255)	'$.project.code',
				IsActive		varchar(10)		'$.project.active',
				Billable		varchar(10)		'$.project.billable',
				BillBy			varchar(255)	'$.project.bill_by',
				Budget			varchar(255)	'$.project.budget',
				BudgetBy		varchar(255)	'$.project.budget_by',
				NotifyOverBudget	varchar(10)	'$.project.notify_when_over_budget',
				OverBudgetNotifyPctage	varchar(20)		'$.project.over_budget_notification_percentage',
				OverBudgetNotifiedAt	varchar(255)	'$.project.over_budget_notified_at',
				ShowBudgetToAll	varchar(10)		'$.project.show_budget_to_all',
				CreatedAt		varchar(50)		'$.project.created_at',
				UpdatedAt		varchar(50)		'$.project.updated_at',
				StartsOn		varchar(50)		'$.project.starts_on',
				EndsOn			varchar(50)		'$.project.ends_on',
				Estimate		varchar(255)	'$.project.estimate',
				EstimateBy		varchar(255)	'$.project.estimate_by',
				HintEarliestRecordAt	varchar(50)	'$.project.hint_earliest_record_at',
				HintLatestRecordAt	varchar(50)	'$.project.hint_latest_record_at',
				Notes			varchar(max)	'$.project.notes',
				HourlyRate		varchar(20)			'$.project.hourly_rate',
				CostBudget		varchar(20)	'$.project.cost_budget',
				CostBudgetInclExpense	varchar(20)	'$.project.cost_budget_include_expenses'
			)) as p
		inner join tblHarvestProjects as hp
			on hp.ProjectID = p.ProjectID

	--insert new values to time entry table:
	INSERT INTO tblHarvestProjects
	SELECT
		p.*
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				ProjectID		varchar(50)		'$.project.id',
				ClientID		varchar(50)		'$.project.client_id',
				ProjectName		varchar(255)	'$.project.name',
				ProjectCode		varchar(255)	'$.project.code',
				IsActive		varchar(10)		'$.project.active',
				Billable		varchar(10)		'$.project.billable',
				BillBy			varchar(255)	'$.project.bill_by',
				Budget			varchar(255)	'$.project.budget',
				BudgetBy		varchar(255)	'$.project.budget_by',
				NotifyOverBudget	varchar(10)	'$.project.notify_when_over_budget',
				OverBudgetNotifyPctage	varchar(20)		'$.project.over_budget_notification_percentage',
				OverBudgetNotifiedAt	varchar(255)	'$.project.over_budget_notified_at',
				ShowBudgetToAll	varchar(10)		'$.project.show_budget_to_all',
				CreatedAt		varchar(50)		'$.project.created_at',
				UpdatedAt		varchar(50)		'$.project.updated_at',
				StartsOn		varchar(50)		'$.project.starts_on',
				EndsOn			varchar(50)		'$.project.ends_on',
				Estimate		varchar(255)	'$.project.estimate',
				EstimateBy		varchar(255)	'$.project.estimate_by',
				HintEarliestRecordAt	varchar(50)	'$.project.hint_earliest_record_at',
				HintLatestRecordAt	varchar(50)	'$.project.hint_latest_record_at',
				Notes			varchar(max)	'$.project.notes',
				HourlyRate		varchar(20)			'$.project.hourly_rate',
				CostBudget		varchar(20)	'$.project.cost_budget',
				CostBudgetInclExpense	varchar(20)	'$.project.cost_budget_include_expenses'
			)) as p
		left join tblHarvestProjects as hp
			on hp.ProjectID = p.ProjectID
		WHERE
			hp.ProjectID IS NULL



GO
/****** Object:  StoredProcedure [dbo].[spHarvestTaskConcatenation]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE procedure [dbo].[spHarvestTaskConcatenation] (
	@Startdate datetime,
	@EndDate datetime)

as
/*
 Author:		Derek DeLange
 Date Created:	2017-02-10
 Description:	Bundles tasks of the same category for the
				same client on the same date into one line.

 Change Log:
 Date			Changed By				Description
 ==================================================
 2017-02-10		DeLange, Derek			Created SP off new tables

 ==================================================
 */

with cte([date],employee,client,project,task,[hours],notes,rn)
as
(
	select 
		ht.TaskDate,
		hu.LastName + ', ' + hu.FirstName as employee,
		ht.Client,
		ht.Project,
		ht.TaskName,
		ht.TaskEntryHours,
		ht.notes,
		rn=ROW_NUMBER() over (PARTITION by ht.TaskDate, hu.LastName + ', ' + hu.FirstName, ht.client, ht.project, ht.TaskName order by ht.TaskDate)
	from 
		tblHarvestTimeEntries ht
		left join
		tblHarvestUsers hu
			on ht.UserID = hu.UserID
	where
		ht.TaskDate between @Startdate and @EndDate
)
,cte2([date],employee,client,project,task,[hours],mergednotes,rn)
as
(
select 
	[Date],
	employee,
	Client,
	Project,
	task,
	[hours],
	convert(varchar(max),notes), 
	1
from 
	cte 
where 
	rn=1
union all
select 
	cte2.[Date],
	cte2.employee, 
	cte2.Client,
	cte2.project,
	cte2.task,
	cte2.[hours] + cte.[hours],
	convert(varchar(max),
	cte2.mergednotes+'; '+cte.notes), 
	cte2.rn+1
from 
	cte2
	inner join 
	cte 
		on cte.[date] = cte2.[date]
		and cte.employee = cte2.employee
		and cte.client = cte2.client
		and cte.project = cte2.project 
		and cte.task = cte2.task
		and cte.rn=cte2.rn+1
)
select [date], employee, client, project, task, max([hours]) as [Hours], max(mergednotes) as [Description] from cte2 group by [date], employee, client, project, task




GO
/****** Object:  StoredProcedure [dbo].[spHarvestTaskConcatenation_BillableOnly]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







CREATE procedure [dbo].[spHarvestTaskConcatenation_BillableOnly] (
	@Startdate datetime,
	@EndDate datetime)

as
/*
 Author:		Derek DeLange
 Date Created:	2017-02-10
 Description:	Bundles tasks of the same category for the
				same client on the same date into one line.

 Change Log:
 Date			Changed By				Description
 ========================================================================================================
 2017-02-10		DeLange, Derek			Created SP off new tables
 2017-03-03		DeLange, Derek			Modified billable scope to exclude "Business Development"
 2017-03-09		DeLange, Derek			Modified to exclude deleted records based on newly-created field.
										Modified to only include billable projects (not just billable tasks)
 ========================================================================================================
 */

with cte([date],employee,client,project,task,[hours],notes,rn)
as
(
	select 
		ht.TaskDate,
		hu.LastName + ', ' + hu.FirstName as employee,
		ht.Client,
		ht.Project,
		ht.TaskName,
		ht.TaskEntryHours,
		ht.notes,
		rn=ROW_NUMBER() over (PARTITION by ht.TaskDate, hu.LastName + ', ' + hu.FirstName, ht.client, ht.project, ht.TaskName order by ht.TaskDate)
	from 
		tblHarvestTimeEntries ht
		left join
		tblHarvestUsers hu
			on ht.UserID = hu.UserID
		left join
		tblHarvestTasks hta
			on ht.TaskID = hta.TaskID
		left join
		tblHarvestProjects hp
			on ht.ProjectID = hp.ProjectID
	where
		ht.TaskDate between @Startdate and @EndDate
		and isnull(ht.RecordDeleted, 0) = 0
		and hta.Billable = 'true'
		and hp.Billable = 'true'
		and ht.Client not in ('TBD Solutions LLC', 'Business Development')
)
,cte2([date],employee,client,project,task,[hours],mergednotes,rn)
as
(
select 
	[Date],
	employee,
	Client,
	Project,
	task,
	[hours],
	convert(varchar(max),notes), 
	1
from 
	cte 
where 
	rn=1
union all
select 
	cte2.[Date],
	cte2.employee, 
	cte2.Client,
	cte2.project,
	cte2.task,
	cte2.[hours] + cte.[hours],
	convert(varchar(max),
	cte2.mergednotes+'; '+cte.notes), 
	cte2.rn+1
from 
	cte2
	inner join 
	cte 
		on cte.[date] = cte2.[date]
		and cte.employee = cte2.employee
		and cte.client = cte2.client
		and cte.project = cte2.project 
		and cte.task = cte2.task
		and cte.rn=cte2.rn+1
)
select [date], employee, client, project, task, max([hours]) as [Hours], max(mergednotes) as [Description] from cte2 group by [date], employee, client, project, task








GO
/****** Object:  StoredProcedure [dbo].[spHarvestTaskLoad]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE PROCEDURE [dbo].[spHarvestTaskLoad] AS
 /*
 Author:		Derek DeLange
 Date Created:	2017-02-10
 Description:	Processes and loads JSON task records
				downloaded via Python script.

 Change Log:
 Date			Changed By				Description
 ==================================================
 2017-02-10		DeLange, Derek			Created SP

 ==================================================
 */

DECLARE @json varchar(max) = (select bulkcolumn from openrowset(BULK 'C:\HarvestImport\taskData.json', SINGLE_CLOB) as j)

	--update existing values in tasks table:
	UPDATE ht
	SET
		ht.TaskID = t.TaskID,
		ht.TaskName = t.TaskName,
		ht.Billable = t.BillableByDefault,
		ht.CreatedAt = t.CreatedAt,
		ht.UpdatedAt = t.UpdatedAt,
		ht.IsDefault = t.IsDefault,
		ht.DefaultHourlyRate = t.DefaultHrlyRate,
		ht.Deactivated = t.Deactivated
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				TaskID			varchar(50)		'$.task.id',
				TaskName		varchar(200)	'$.task.name',
				BillableByDefault	varchar(10)	'$.task.billable_by_default',
				CreatedAt		varchar(50)		'$.task.created_at',
				UpdatedAt		varchar(50)		'$.task.updated_at',
				IsDefault		varchar(10)		'$.task.is_default',
				DefaultHrlyRate	real			'$.task.default_hourly_rate',
				Deactivated		varchar(10)		'$.task.deactivated'
			)) as t
		inner join tblHarvestTasks as ht
			on ht.TaskID = t.TaskID

	--insert new values to tasks table:
	INSERT INTO tblHarvestTasks
	SELECT
		t.*
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				TaskID			varchar(50)		'$.task.id',
				TaskName		varchar(200)	'$.task.name',
				BillableByDefault	varchar(10)	'$.task.billable_by_default',
				CreatedAt		varchar(50)		'$.task.created_at',
				UpdatedAt		varchar(50)		'$.task.updated_at',
				IsDefault		varchar(10)		'$.task.is_default',
				DefaultHrlyRate	real			'$.task.default_hourly_rate',
				Deactivated		varchar(10)		'$.task.deactivated'
			)) as t
		left join tblHarvestTasks as ht
			on ht.TaskID = t.TaskID
		WHERE
			ht.TaskID IS NULL



GO
/****** Object:  StoredProcedure [dbo].[spHarvestTimeEntryLoad]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


 CREATE PROCEDURE [dbo].[spHarvestTimeEntryLoad] AS
 /*
 Author:		Derek DeLange
 Date Created:	2017-02-09
 Description:	Processes and loads JSON time entry files 
				downloaded via Python script.

 Change Log:
 Date			Changed By				Description
 ====================================================================================
 2017-02-09		DeLange, Derek			Created SP
 2017-03-08		DeLange, Derek			Modified to include soft-delete functionality
										- added current date, current user variables
 ====================================================================================
 */
 IF OBJECT_ID('tempdb..#JSONTimeEntryFiles') IS NOT NULL
    DROP TABLE #JSONTimeEntryFiles


 CREATE TABLE #JSONTimeEntryFiles (
	ID			int				identity(1,1),
	JSONQuery	varchar(max),
	FName		varchar(50),
	depth		int,
	isfile		int)

DECLARE @jsonfolder SYSNAME = 'C:\HarvestImport\SplitJSON\'
DECLARE @currentfile varchar(max)
DECLARE @json varchar(max)
DECLARE @filepath varchar(max)
DECLARE @dynSQL varchar(max)
DECLARE @currentdate datetime
DECLARE @currentuser varchar(25)

INSERT INTO #JSONTimeEntryFiles (FName, depth, isfile)
	EXEC Master..xp_dirtree @jsonfolder, 10, 1

DECLARE JSONFileCursor CURSOR FOR
	SELECT 
		FName 
	FROM
		#JSONTimeEntryFiles

OPEN JSONFileCursor
FETCH NEXT FROM JSONFileCursor INTO @CurrentFile

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @filepath = @jsonfolder + @currentfile
	SET @dynSQL = 'update #JSONTimeEntryFiles set JSONQuery = (select bulkcolumn from openrowset(BULK ''' + @filepath + ''', SINGLE_CLOB) as j) where (FName = ''' + @currentfile + ''')'
	EXEC (@dynSQL)
	SET @json = (select jsonquery from #JSONTimeEntryFiles where FName = @currentfile)

	--populate the current user/date variables
	SELECT
		@currentuser = JSON_VALUE(d.value, '$.user_id'),
		@currentdate = JSON_VALUE(d.value, '$.spent_at')
	FROM
		OPENJSON(@json, '$.day_entries') as d

	--update existing values in time entry table:
	UPDATE hte
	SET
		hte.UserID = JSON_VALUE(d.value, '$.user_id'),
		hte.TaskDate = JSON_VALUE(d.value, '$.spent_at'),
		hte.ProjectID = JSON_VALUE(d.value, '$.project_id'),
		hte.TaskID = JSON_VALUE(d.value, '$.task_id'),
		hte.Project = JSON_VALUE(d.value, '$.project'),
		hte.TaskName = JSON_VALUE(d.value, '$.task'),
		hte.Client = JSON_VALUE(d.value, '$.client'),
		hte.Notes = JSON_VALUE(d.value, '$.notes'),
		hte.TaskEntryHours = JSON_VALUE(d.value, '$.hours')
	FROM
		OPENJSON(@json, '$.day_entries') as d
		inner join tblHarvestTimeEntries as hte
			on hte.TaskEntryID = JSON_VALUE(d.value, '$.id')

	--insert new values to time entry table:
	INSERT INTO tblHarvestTimeEntries 
	SELECT
		JSON_VALUE(d.value, '$.id') as TaskEntryID,
		JSON_VALUE(d.value, '$.user_id') as UserID,
		JSON_VALUE(d.value, '$.spent_at') as TaskDate,
		JSON_VALUE(d.value, '$.project_id') as ProjectID,
		JSON_VALUE(d.value, '$.task_id') as TaskID,
		JSON_VALUE(d.value, '$.project') as ProjectName,
		JSON_VALUE(d.value, '$.task') as TaskName,
		JSON_VALUE(d.value, '$.client') as ClientName,
		JSON_VALUE(d.value, '$.notes') as TaskNotes,
		JSON_VALUE(d.value, '$.hours') as TaskHours,
		0,
		NULL
	FROM
		OPENJSON(@json, '$.day_entries') as d
		left join
		tblHarvestTimeEntries h1
			ON JSON_VALUE(d.value, '$.id') = h1.TaskEntryID
	WHERE
		h1.TaskEntryID IS NULL

	--soft-delete removed values:
	UPDATE hte
	SET
		hte.RecordDeleted = 1,
		hte.DeletedDate = GETDATE()
	FROM
		OPENJSON(@json, '$.day_entries') as d
		right join tblHarvestTimeEntries as hte
			on hte.TaskEntryID = JSON_VALUE(d.value, '$.id')
			and hte.TaskDate = JSON_VALUE(d.value, '$.spent_at')
			and hte.UserID = JSON_VALUE(d.value, '$.user_id')
	WHERE
		hte.userID = @currentuser
		and hte.TaskDate = @currentdate
		and JSON_VALUE(d.value, '$.id') is null

	--reset variables:
	set @currentdate = null
	set @currentuser = null

	FETCH NEXT FROM JSONFileCursor INTO @CurrentFile
		
END

CLOSE JSONFileCursor
DEALLOCATE JSONFileCursor
DROP TABLE #JSONTimeEntryFiles


GO
/****** Object:  StoredProcedure [dbo].[spHarvestUserLoad]    Script Date: 3/20/2017 2:15:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE PROCEDURE [dbo].[spHarvestUserLoad] AS
 /*
 Author:		Derek DeLange
 Date Created:	2017-02-09
 Description:	Processes and loads JSON user records
				downloaded via Python script.

 Change Log:
 Date			Changed By				Description
 ==================================================
 2017-02-09		DeLange, Derek			Created SP

 ==================================================
 */

DECLARE @json varchar(max) = (select bulkcolumn from openrowset(BULK 'C:\HarvestImport\userData.json', SINGLE_CLOB) as j)

	--update existing values in time entry table:
	UPDATE hu
	SET
		hu.UserID = u.userID,
		hu.FirstName = u.Fname,
		hu.LastName = u.Lname,
		hu.WeeklyCapacity = u.WklyCapacity,
		hu.WantsNewsletter = u.WantsNL,
		hu.UpdatedAt = u.UpdatedAt,
		hu.CreatedAt = u.CreatedAt,
		hu.IsContractor = u.IsContractor,
		hu.IsActive = u.IsActive,
		hu.Telephone = u.Telephone,
		hu.Email = u.Email,
		hu.DefaultHrlyRate = u.HourlyRate,
		hu.CostRate = u.CostRate,
		hu.IsAdmin = u.IsAdmin,
		hu.HasAccessToFutureProjects = u.AccessFtrProj,
		hu.Department = u.Department,
		hu.Timezone = u.TimeZone
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				UserID			varchar(50)		'$.user.id',
				Fname			varchar(200)	'$.user.first_name',
				Lname			varchar(200)	'$.user.last_name',
				WklyCapacity	int				'$.user.weekly_capacity',
				WantsNL			varchar(10)		'$.user.wants_newsletter',
				UpdatedAt		varchar(50)		'$.user.updated_at',
				CreatedAt		varchar(50)		'$.user.created_at',
				IsContractor	varchar(10)		'$.user.is_contractor',
				IsActive		varchar(10)		'$.user.is_active',
				Telephone		varchar(25)		'$.user.telephone',
				Email			varchar(50)		'$.user.email',
				HourlyRate		real			'$.user.default_hourly_rate',
				CostRate		real			'$.user.cost_rate',
				IsAdmin			varchar(10)		'$.user.is_admin',
				AccessFtrProj	varchar(10)		'$.user.has_access_to_all_future_projects',
				Department		varchar(200)	'$.user.department',
				TimeZone		varchar(200)	'$.user.timezone'
			)) as u
		inner join tblHarvestUsers as hu
			on hu.UserID = u.UserID

	--insert new values to time entry table:
	INSERT INTO tblHarvestUsers
	SELECT
		u.*
	FROM
		(SELECT
			* 
		FROM
			OPENJSON(@json)
			WITH (
				UserID			varchar(50)		'$.user.id',
				Fname			varchar(200)	'$.user.first_name',
				Lname			varchar(200)	'$.user.last_name',
				WklyCapacity	int				'$.user.weekly_capacity',
				WantsNL			varchar(10)		'$.user.wants_newsletter',
				UpdatedAt		varchar(50)		'$.user.updated_at',
				CreatedAt		varchar(50)		'$.user.created_at',
				IsContractor	varchar(10)		'$.user.is_contractor',
				IsActive		varchar(10)		'$.user.is_active',
				Telephone		varchar(25)		'$.user.telephone',
				Email			varchar(50)		'$.user.email',
				HourlyRate		real			'$.user.default_hourly_rate',
				CostRate		real			'$.user.cost_rate',
				IsAdmin			varchar(10)		'$.user.is_admin',
				AccessFtrProj	varchar(10)		'$.user.has_access_to_all_future_projects',
				Department		varchar(200)	'$.user.department',
				TimeZone		varchar(200)	'$.user.timezon'
			)) as u
		left join tblHarvestUsers as hu
			on hu.UserID = u.UserID
		WHERE
			hu.UserID IS NULL



GO
USE [master]
GO
ALTER DATABASE [TBDS-Internal] SET  READ_WRITE 
GO
