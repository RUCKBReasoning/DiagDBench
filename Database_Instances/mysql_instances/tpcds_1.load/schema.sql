-- MySQL dump 10.13  Distrib 8.4.9, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: tpcds_1
-- ------------------------------------------------------
-- Server version	8.4.9

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `tpcds_1`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `tpcds_1` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE `tpcds_1`;


--
-- Table structure for table `call_center`
--

DROP TABLE IF EXISTS `call_center`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `call_center` (
  `cc_call_center_sk` int NOT NULL,
  `cc_call_center_id` char(16) NOT NULL,
  `cc_rec_start_date` date DEFAULT NULL,
  `cc_rec_end_date` date DEFAULT NULL,
  `cc_closed_date_sk` int DEFAULT NULL,
  `cc_open_date_sk` int DEFAULT NULL,
  `cc_name` varchar(50) DEFAULT NULL,
  `cc_class` varchar(50) DEFAULT NULL,
  `cc_employees` int DEFAULT NULL,
  `cc_sq_ft` int DEFAULT NULL,
  `cc_hours` char(20) DEFAULT NULL,
  `cc_manager` varchar(40) DEFAULT NULL,
  `cc_mkt_id` int DEFAULT NULL,
  `cc_mkt_class` char(50) DEFAULT NULL,
  `cc_mkt_desc` varchar(100) DEFAULT NULL,
  `cc_market_manager` varchar(40) DEFAULT NULL,
  `cc_division` int DEFAULT NULL,
  `cc_division_name` varchar(50) DEFAULT NULL,
  `cc_company` int DEFAULT NULL,
  `cc_company_name` char(50) DEFAULT NULL,
  `cc_street_number` char(10) DEFAULT NULL,
  `cc_street_name` varchar(60) DEFAULT NULL,
  `cc_street_type` char(15) DEFAULT NULL,
  `cc_suite_number` char(10) DEFAULT NULL,
  `cc_city` varchar(60) DEFAULT NULL,
  `cc_county` varchar(30) DEFAULT NULL,
  `cc_state` char(2) DEFAULT NULL,
  `cc_zip` char(10) DEFAULT NULL,
  `cc_country` varchar(20) DEFAULT NULL,
  `cc_gmt_offset` decimal(5,2) DEFAULT NULL,
  `cc_tax_percentage` decimal(5,2) DEFAULT NULL,
  PRIMARY KEY (`cc_call_center_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `call_center`
--


--
-- Table structure for table `catalog_page`
--

DROP TABLE IF EXISTS `catalog_page`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_page` (
  `cp_catalog_page_sk` int NOT NULL,
  `cp_catalog_page_id` char(16) NOT NULL,
  `cp_start_date_sk` int DEFAULT NULL,
  `cp_end_date_sk` int DEFAULT NULL,
  `cp_department` varchar(50) DEFAULT NULL,
  `cp_catalog_number` int DEFAULT NULL,
  `cp_catalog_page_number` int DEFAULT NULL,
  `cp_description` varchar(100) DEFAULT NULL,
  `cp_type` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`cp_catalog_page_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `catalog_page`
--


--
-- Table structure for table `catalog_returns`
--

DROP TABLE IF EXISTS `catalog_returns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_returns` (
  `cr_returned_date_sk` int DEFAULT NULL,
  `cr_returned_time_sk` int DEFAULT NULL,
  `cr_item_sk` int NOT NULL,
  `cr_refunded_customer_sk` int DEFAULT NULL,
  `cr_refunded_cdemo_sk` int DEFAULT NULL,
  `cr_refunded_hdemo_sk` int DEFAULT NULL,
  `cr_refunded_addr_sk` int DEFAULT NULL,
  `cr_returning_customer_sk` int DEFAULT NULL,
  `cr_returning_cdemo_sk` int DEFAULT NULL,
  `cr_returning_hdemo_sk` int DEFAULT NULL,
  `cr_returning_addr_sk` int DEFAULT NULL,
  `cr_call_center_sk` int DEFAULT NULL,
  `cr_catalog_page_sk` int DEFAULT NULL,
  `cr_ship_mode_sk` int DEFAULT NULL,
  `cr_warehouse_sk` int DEFAULT NULL,
  `cr_reason_sk` int DEFAULT NULL,
  `cr_order_number` int NOT NULL,
  `cr_return_quantity` int DEFAULT NULL,
  `cr_return_amount` decimal(7,2) DEFAULT NULL,
  `cr_return_tax` decimal(7,2) DEFAULT NULL,
  `cr_return_amt_inc_tax` decimal(7,2) DEFAULT NULL,
  `cr_fee` decimal(7,2) DEFAULT NULL,
  `cr_return_ship_cost` decimal(7,2) DEFAULT NULL,
  `cr_refunded_cash` decimal(7,2) DEFAULT NULL,
  `cr_reversed_charge` decimal(7,2) DEFAULT NULL,
  `cr_store_credit` decimal(7,2) DEFAULT NULL,
  `cr_net_loss` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`cr_item_sk`,`cr_order_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `catalog_returns`
--


--
-- Table structure for table `catalog_sales`
--

DROP TABLE IF EXISTS `catalog_sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_sales` (
  `cs_sold_date_sk` int DEFAULT NULL,
  `cs_sold_time_sk` int DEFAULT NULL,
  `cs_ship_date_sk` int DEFAULT NULL,
  `cs_bill_customer_sk` int DEFAULT NULL,
  `cs_bill_cdemo_sk` int DEFAULT NULL,
  `cs_bill_hdemo_sk` int DEFAULT NULL,
  `cs_bill_addr_sk` int DEFAULT NULL,
  `cs_ship_customer_sk` int DEFAULT NULL,
  `cs_ship_cdemo_sk` int DEFAULT NULL,
  `cs_ship_hdemo_sk` int DEFAULT NULL,
  `cs_ship_addr_sk` int DEFAULT NULL,
  `cs_call_center_sk` int DEFAULT NULL,
  `cs_catalog_page_sk` int DEFAULT NULL,
  `cs_ship_mode_sk` int DEFAULT NULL,
  `cs_warehouse_sk` int DEFAULT NULL,
  `cs_item_sk` int NOT NULL,
  `cs_promo_sk` int DEFAULT NULL,
  `cs_order_number` int NOT NULL,
  `cs_quantity` int DEFAULT NULL,
  `cs_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `cs_list_price` decimal(7,2) DEFAULT NULL,
  `cs_sales_price` decimal(7,2) DEFAULT NULL,
  `cs_ext_discount_amt` decimal(7,2) DEFAULT NULL,
  `cs_ext_sales_price` decimal(7,2) DEFAULT NULL,
  `cs_ext_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `cs_ext_list_price` decimal(7,2) DEFAULT NULL,
  `cs_ext_tax` decimal(7,2) DEFAULT NULL,
  `cs_coupon_amt` decimal(7,2) DEFAULT NULL,
  `cs_ext_ship_cost` decimal(7,2) DEFAULT NULL,
  `cs_net_paid` decimal(7,2) DEFAULT NULL,
  `cs_net_paid_inc_tax` decimal(7,2) DEFAULT NULL,
  `cs_net_paid_inc_ship` decimal(7,2) DEFAULT NULL,
  `cs_net_paid_inc_ship_tax` decimal(7,2) DEFAULT NULL,
  `cs_net_profit` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`cs_item_sk`,`cs_order_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `catalog_sales`
--


--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `customer` (
  `c_customer_sk` int NOT NULL,
  `c_customer_id` char(16) NOT NULL,
  `c_current_cdemo_sk` int DEFAULT NULL,
  `c_current_hdemo_sk` int DEFAULT NULL,
  `c_current_addr_sk` int DEFAULT NULL,
  `c_first_shipto_date_sk` int DEFAULT NULL,
  `c_first_sales_date_sk` int DEFAULT NULL,
  `c_salutation` char(10) DEFAULT NULL,
  `c_first_name` char(20) DEFAULT NULL,
  `c_last_name` char(30) DEFAULT NULL,
  `c_preferred_cust_flag` char(1) DEFAULT NULL,
  `c_birth_day` int DEFAULT NULL,
  `c_birth_month` int DEFAULT NULL,
  `c_birth_year` int DEFAULT NULL,
  `c_birth_country` varchar(20) DEFAULT NULL,
  `c_login` char(13) DEFAULT NULL,
  `c_email_address` char(50) DEFAULT NULL,
  `c_last_review_date_sk` int DEFAULT NULL,
  PRIMARY KEY (`c_customer_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `customer`
--


--
-- Table structure for table `customer_address`
--

DROP TABLE IF EXISTS `customer_address`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `customer_address` (
  `ca_address_sk` int NOT NULL,
  `ca_address_id` char(16) NOT NULL,
  `ca_street_number` char(10) DEFAULT NULL,
  `ca_street_name` varchar(60) DEFAULT NULL,
  `ca_street_type` char(15) DEFAULT NULL,
  `ca_suite_number` char(10) DEFAULT NULL,
  `ca_city` varchar(60) DEFAULT NULL,
  `ca_county` varchar(30) DEFAULT NULL,
  `ca_state` char(2) DEFAULT NULL,
  `ca_zip` char(10) DEFAULT NULL,
  `ca_country` varchar(20) DEFAULT NULL,
  `ca_gmt_offset` decimal(5,2) DEFAULT NULL,
  `ca_location_type` char(20) DEFAULT NULL,
  PRIMARY KEY (`ca_address_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `customer_address`
--


--
-- Table structure for table `customer_demographics`
--

DROP TABLE IF EXISTS `customer_demographics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `customer_demographics` (
  `cd_demo_sk` int NOT NULL,
  `cd_gender` char(1) DEFAULT NULL,
  `cd_marital_status` char(1) DEFAULT NULL,
  `cd_education_status` char(20) DEFAULT NULL,
  `cd_purchase_estimate` int DEFAULT NULL,
  `cd_credit_rating` char(10) DEFAULT NULL,
  `cd_dep_count` int DEFAULT NULL,
  `cd_dep_employed_count` int DEFAULT NULL,
  `cd_dep_college_count` int DEFAULT NULL,
  PRIMARY KEY (`cd_demo_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `customer_demographics`
--


--
-- Table structure for table `date_dim`
--

DROP TABLE IF EXISTS `date_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `date_dim` (
  `d_date_sk` int NOT NULL,
  `d_date_id` char(16) NOT NULL,
  `d_date` date DEFAULT NULL,
  `d_month_seq` int DEFAULT NULL,
  `d_week_seq` int DEFAULT NULL,
  `d_quarter_seq` int DEFAULT NULL,
  `d_year` int DEFAULT NULL,
  `d_dow` int DEFAULT NULL,
  `d_moy` int DEFAULT NULL,
  `d_dom` int DEFAULT NULL,
  `d_qoy` int DEFAULT NULL,
  `d_fy_year` int DEFAULT NULL,
  `d_fy_quarter_seq` int DEFAULT NULL,
  `d_fy_week_seq` int DEFAULT NULL,
  `d_day_name` char(9) DEFAULT NULL,
  `d_quarter_name` char(6) DEFAULT NULL,
  `d_holiday` char(1) DEFAULT NULL,
  `d_weekend` char(1) DEFAULT NULL,
  `d_following_holiday` char(1) DEFAULT NULL,
  `d_first_dom` int DEFAULT NULL,
  `d_last_dom` int DEFAULT NULL,
  `d_same_day_ly` int DEFAULT NULL,
  `d_same_day_lq` int DEFAULT NULL,
  `d_current_day` char(1) DEFAULT NULL,
  `d_current_week` char(1) DEFAULT NULL,
  `d_current_month` char(1) DEFAULT NULL,
  `d_current_quarter` char(1) DEFAULT NULL,
  `d_current_year` char(1) DEFAULT NULL,
  PRIMARY KEY (`d_date_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `date_dim`
--


--
-- Table structure for table `dbgen_version`
--

DROP TABLE IF EXISTS `dbgen_version`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dbgen_version` (
  `dv_version` varchar(16) DEFAULT NULL,
  `dv_create_date` date DEFAULT NULL,
  `dv_create_time` time DEFAULT NULL,
  `dv_cmdline_args` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dbgen_version`
--


--
-- Table structure for table `household_demographics`
--

DROP TABLE IF EXISTS `household_demographics`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `household_demographics` (
  `hd_demo_sk` int NOT NULL,
  `hd_income_band_sk` int DEFAULT NULL,
  `hd_buy_potential` char(15) DEFAULT NULL,
  `hd_dep_count` int DEFAULT NULL,
  `hd_vehicle_count` int DEFAULT NULL,
  PRIMARY KEY (`hd_demo_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `household_demographics`
--


--
-- Table structure for table `income_band`
--

DROP TABLE IF EXISTS `income_band`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `income_band` (
  `ib_income_band_sk` int NOT NULL,
  `ib_lower_bound` int DEFAULT NULL,
  `ib_upper_bound` int DEFAULT NULL,
  PRIMARY KEY (`ib_income_band_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `income_band`
--


--
-- Table structure for table `inventory`
--

DROP TABLE IF EXISTS `inventory`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `inventory` (
  `inv_date_sk` int NOT NULL,
  `inv_item_sk` int NOT NULL,
  `inv_warehouse_sk` int NOT NULL,
  `inv_quantity_on_hand` int DEFAULT NULL,
  PRIMARY KEY (`inv_date_sk`,`inv_item_sk`,`inv_warehouse_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `inventory`
--


--
-- Table structure for table `item`
--

DROP TABLE IF EXISTS `item`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `item` (
  `i_item_sk` int NOT NULL,
  `i_item_id` char(16) NOT NULL,
  `i_rec_start_date` date DEFAULT NULL,
  `i_rec_end_date` date DEFAULT NULL,
  `i_item_desc` varchar(200) DEFAULT NULL,
  `i_current_price` decimal(7,2) DEFAULT NULL,
  `i_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `i_brand_id` int DEFAULT NULL,
  `i_brand` char(50) DEFAULT NULL,
  `i_class_id` int DEFAULT NULL,
  `i_class` char(50) DEFAULT NULL,
  `i_category_id` int DEFAULT NULL,
  `i_category` char(50) DEFAULT NULL,
  `i_manufact_id` int DEFAULT NULL,
  `i_manufact` char(50) DEFAULT NULL,
  `i_size` char(20) DEFAULT NULL,
  `i_formulation` char(20) DEFAULT NULL,
  `i_color` char(20) DEFAULT NULL,
  `i_units` char(10) DEFAULT NULL,
  `i_container` char(10) DEFAULT NULL,
  `i_manager_id` int DEFAULT NULL,
  `i_product_name` char(50) DEFAULT NULL,
  PRIMARY KEY (`i_item_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `item`
--


--
-- Table structure for table `promotion`
--

DROP TABLE IF EXISTS `promotion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `promotion` (
  `p_promo_sk` int NOT NULL,
  `p_promo_id` char(16) NOT NULL,
  `p_start_date_sk` int DEFAULT NULL,
  `p_end_date_sk` int DEFAULT NULL,
  `p_item_sk` int DEFAULT NULL,
  `p_cost` decimal(15,2) DEFAULT NULL,
  `p_response_target` int DEFAULT NULL,
  `p_promo_name` char(50) DEFAULT NULL,
  `p_channel_dmail` char(1) DEFAULT NULL,
  `p_channel_email` char(1) DEFAULT NULL,
  `p_channel_catalog` char(1) DEFAULT NULL,
  `p_channel_tv` char(1) DEFAULT NULL,
  `p_channel_radio` char(1) DEFAULT NULL,
  `p_channel_press` char(1) DEFAULT NULL,
  `p_channel_event` char(1) DEFAULT NULL,
  `p_channel_demo` char(1) DEFAULT NULL,
  `p_channel_details` varchar(100) DEFAULT NULL,
  `p_purpose` char(15) DEFAULT NULL,
  `p_discount_active` char(1) DEFAULT NULL,
  PRIMARY KEY (`p_promo_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `promotion`
--


--
-- Table structure for table `reason`
--

DROP TABLE IF EXISTS `reason`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `reason` (
  `r_reason_sk` int NOT NULL,
  `r_reason_id` char(16) NOT NULL,
  `r_reason_desc` char(100) DEFAULT NULL,
  PRIMARY KEY (`r_reason_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `reason`
--


--
-- Table structure for table `ship_mode`
--

DROP TABLE IF EXISTS `ship_mode`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `ship_mode` (
  `sm_ship_mode_sk` int NOT NULL,
  `sm_ship_mode_id` char(16) NOT NULL,
  `sm_type` char(30) DEFAULT NULL,
  `sm_code` char(10) DEFAULT NULL,
  `sm_carrier` char(20) DEFAULT NULL,
  `sm_contract` char(20) DEFAULT NULL,
  PRIMARY KEY (`sm_ship_mode_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ship_mode`
--


--
-- Table structure for table `store`
--

DROP TABLE IF EXISTS `store`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `store` (
  `s_store_sk` int NOT NULL,
  `s_store_id` char(16) NOT NULL,
  `s_rec_start_date` date DEFAULT NULL,
  `s_rec_end_date` date DEFAULT NULL,
  `s_closed_date_sk` int DEFAULT NULL,
  `s_store_name` varchar(50) DEFAULT NULL,
  `s_number_employees` int DEFAULT NULL,
  `s_floor_space` int DEFAULT NULL,
  `s_hours` char(20) DEFAULT NULL,
  `s_manager` varchar(40) DEFAULT NULL,
  `s_market_id` int DEFAULT NULL,
  `s_geography_class` varchar(100) DEFAULT NULL,
  `s_market_desc` varchar(100) DEFAULT NULL,
  `s_market_manager` varchar(40) DEFAULT NULL,
  `s_division_id` int DEFAULT NULL,
  `s_division_name` varchar(50) DEFAULT NULL,
  `s_company_id` int DEFAULT NULL,
  `s_company_name` varchar(50) DEFAULT NULL,
  `s_street_number` varchar(10) DEFAULT NULL,
  `s_street_name` varchar(60) DEFAULT NULL,
  `s_street_type` char(15) DEFAULT NULL,
  `s_suite_number` char(10) DEFAULT NULL,
  `s_city` varchar(60) DEFAULT NULL,
  `s_county` varchar(30) DEFAULT NULL,
  `s_state` char(2) DEFAULT NULL,
  `s_zip` char(10) DEFAULT NULL,
  `s_country` varchar(20) DEFAULT NULL,
  `s_gmt_offset` decimal(5,2) DEFAULT NULL,
  `s_tax_precentage` decimal(5,2) DEFAULT NULL,
  PRIMARY KEY (`s_store_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `store`
--


--
-- Table structure for table `store_returns`
--

DROP TABLE IF EXISTS `store_returns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `store_returns` (
  `sr_returned_date_sk` int DEFAULT NULL,
  `sr_return_time_sk` int DEFAULT NULL,
  `sr_item_sk` int NOT NULL,
  `sr_customer_sk` int DEFAULT NULL,
  `sr_cdemo_sk` int DEFAULT NULL,
  `sr_hdemo_sk` int DEFAULT NULL,
  `sr_addr_sk` int DEFAULT NULL,
  `sr_store_sk` int DEFAULT NULL,
  `sr_reason_sk` int DEFAULT NULL,
  `sr_ticket_number` int NOT NULL,
  `sr_return_quantity` int DEFAULT NULL,
  `sr_return_amt` decimal(7,2) DEFAULT NULL,
  `sr_return_tax` decimal(7,2) DEFAULT NULL,
  `sr_return_amt_inc_tax` decimal(7,2) DEFAULT NULL,
  `sr_fee` decimal(7,2) DEFAULT NULL,
  `sr_return_ship_cost` decimal(7,2) DEFAULT NULL,
  `sr_refunded_cash` decimal(7,2) DEFAULT NULL,
  `sr_reversed_charge` decimal(7,2) DEFAULT NULL,
  `sr_store_credit` decimal(7,2) DEFAULT NULL,
  `sr_net_loss` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`sr_item_sk`,`sr_ticket_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `store_returns`
--


--
-- Table structure for table `store_sales`
--

DROP TABLE IF EXISTS `store_sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `store_sales` (
  `ss_sold_date_sk` int DEFAULT NULL,
  `ss_sold_time_sk` int DEFAULT NULL,
  `ss_item_sk` int NOT NULL,
  `ss_customer_sk` int DEFAULT NULL,
  `ss_cdemo_sk` int DEFAULT NULL,
  `ss_hdemo_sk` int DEFAULT NULL,
  `ss_addr_sk` int DEFAULT NULL,
  `ss_store_sk` int DEFAULT NULL,
  `ss_promo_sk` int DEFAULT NULL,
  `ss_ticket_number` int NOT NULL,
  `ss_quantity` int DEFAULT NULL,
  `ss_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `ss_list_price` decimal(7,2) DEFAULT NULL,
  `ss_sales_price` decimal(7,2) DEFAULT NULL,
  `ss_ext_discount_amt` decimal(7,2) DEFAULT NULL,
  `ss_ext_sales_price` decimal(7,2) DEFAULT NULL,
  `ss_ext_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `ss_ext_list_price` decimal(7,2) DEFAULT NULL,
  `ss_ext_tax` decimal(7,2) DEFAULT NULL,
  `ss_coupon_amt` decimal(7,2) DEFAULT NULL,
  `ss_net_paid` decimal(7,2) DEFAULT NULL,
  `ss_net_paid_inc_tax` decimal(7,2) DEFAULT NULL,
  `ss_net_profit` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`ss_item_sk`,`ss_ticket_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `store_sales`
--


--
-- Table structure for table `time_dim`
--

DROP TABLE IF EXISTS `time_dim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `time_dim` (
  `t_time_sk` int NOT NULL,
  `t_time_id` char(16) NOT NULL,
  `t_time` int DEFAULT NULL,
  `t_hour` int DEFAULT NULL,
  `t_minute` int DEFAULT NULL,
  `t_second` int DEFAULT NULL,
  `t_am_pm` char(2) DEFAULT NULL,
  `t_shift` char(20) DEFAULT NULL,
  `t_sub_shift` char(20) DEFAULT NULL,
  `t_meal_time` char(20) DEFAULT NULL,
  PRIMARY KEY (`t_time_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `time_dim`
--


--
-- Table structure for table `warehouse`
--

DROP TABLE IF EXISTS `warehouse`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `warehouse` (
  `w_warehouse_sk` int NOT NULL,
  `w_warehouse_id` char(16) NOT NULL,
  `w_warehouse_name` varchar(20) DEFAULT NULL,
  `w_warehouse_sq_ft` int DEFAULT NULL,
  `w_street_number` char(10) DEFAULT NULL,
  `w_street_name` varchar(60) DEFAULT NULL,
  `w_street_type` char(15) DEFAULT NULL,
  `w_suite_number` char(10) DEFAULT NULL,
  `w_city` varchar(60) DEFAULT NULL,
  `w_county` varchar(30) DEFAULT NULL,
  `w_state` char(2) DEFAULT NULL,
  `w_zip` char(10) DEFAULT NULL,
  `w_country` varchar(20) DEFAULT NULL,
  `w_gmt_offset` decimal(5,2) DEFAULT NULL,
  PRIMARY KEY (`w_warehouse_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `warehouse`
--


--
-- Table structure for table `web_page`
--

DROP TABLE IF EXISTS `web_page`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `web_page` (
  `wp_web_page_sk` int NOT NULL,
  `wp_web_page_id` char(16) NOT NULL,
  `wp_rec_start_date` date DEFAULT NULL,
  `wp_rec_end_date` date DEFAULT NULL,
  `wp_creation_date_sk` int DEFAULT NULL,
  `wp_access_date_sk` int DEFAULT NULL,
  `wp_autogen_flag` char(1) DEFAULT NULL,
  `wp_customer_sk` int DEFAULT NULL,
  `wp_url` varchar(100) DEFAULT NULL,
  `wp_type` char(50) DEFAULT NULL,
  `wp_char_count` int DEFAULT NULL,
  `wp_link_count` int DEFAULT NULL,
  `wp_image_count` int DEFAULT NULL,
  `wp_max_ad_count` int DEFAULT NULL,
  PRIMARY KEY (`wp_web_page_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `web_page`
--


--
-- Table structure for table `web_returns`
--

DROP TABLE IF EXISTS `web_returns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `web_returns` (
  `wr_returned_date_sk` int DEFAULT NULL,
  `wr_returned_time_sk` int DEFAULT NULL,
  `wr_item_sk` int NOT NULL,
  `wr_refunded_customer_sk` int DEFAULT NULL,
  `wr_refunded_cdemo_sk` int DEFAULT NULL,
  `wr_refunded_hdemo_sk` int DEFAULT NULL,
  `wr_refunded_addr_sk` int DEFAULT NULL,
  `wr_returning_customer_sk` int DEFAULT NULL,
  `wr_returning_cdemo_sk` int DEFAULT NULL,
  `wr_returning_hdemo_sk` int DEFAULT NULL,
  `wr_returning_addr_sk` int DEFAULT NULL,
  `wr_web_page_sk` int DEFAULT NULL,
  `wr_reason_sk` int DEFAULT NULL,
  `wr_order_number` int NOT NULL,
  `wr_return_quantity` int DEFAULT NULL,
  `wr_return_amt` decimal(7,2) DEFAULT NULL,
  `wr_return_tax` decimal(7,2) DEFAULT NULL,
  `wr_return_amt_inc_tax` decimal(7,2) DEFAULT NULL,
  `wr_fee` decimal(7,2) DEFAULT NULL,
  `wr_return_ship_cost` decimal(7,2) DEFAULT NULL,
  `wr_refunded_cash` decimal(7,2) DEFAULT NULL,
  `wr_reversed_charge` decimal(7,2) DEFAULT NULL,
  `wr_account_credit` decimal(7,2) DEFAULT NULL,
  `wr_net_loss` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`wr_item_sk`,`wr_order_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `web_returns`
--


--
-- Table structure for table `web_sales`
--

DROP TABLE IF EXISTS `web_sales`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `web_sales` (
  `ws_sold_date_sk` int DEFAULT NULL,
  `ws_sold_time_sk` int DEFAULT NULL,
  `ws_ship_date_sk` int DEFAULT NULL,
  `ws_item_sk` int NOT NULL,
  `ws_bill_customer_sk` int DEFAULT NULL,
  `ws_bill_cdemo_sk` int DEFAULT NULL,
  `ws_bill_hdemo_sk` int DEFAULT NULL,
  `ws_bill_addr_sk` int DEFAULT NULL,
  `ws_ship_customer_sk` int DEFAULT NULL,
  `ws_ship_cdemo_sk` int DEFAULT NULL,
  `ws_ship_hdemo_sk` int DEFAULT NULL,
  `ws_ship_addr_sk` int DEFAULT NULL,
  `ws_web_page_sk` int DEFAULT NULL,
  `ws_web_site_sk` int DEFAULT NULL,
  `ws_ship_mode_sk` int DEFAULT NULL,
  `ws_warehouse_sk` int DEFAULT NULL,
  `ws_promo_sk` int DEFAULT NULL,
  `ws_order_number` int NOT NULL,
  `ws_quantity` int DEFAULT NULL,
  `ws_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `ws_list_price` decimal(7,2) DEFAULT NULL,
  `ws_sales_price` decimal(7,2) DEFAULT NULL,
  `ws_ext_discount_amt` decimal(7,2) DEFAULT NULL,
  `ws_ext_sales_price` decimal(7,2) DEFAULT NULL,
  `ws_ext_wholesale_cost` decimal(7,2) DEFAULT NULL,
  `ws_ext_list_price` decimal(7,2) DEFAULT NULL,
  `ws_ext_tax` decimal(7,2) DEFAULT NULL,
  `ws_coupon_amt` decimal(7,2) DEFAULT NULL,
  `ws_ext_ship_cost` decimal(7,2) DEFAULT NULL,
  `ws_net_paid` decimal(7,2) DEFAULT NULL,
  `ws_net_paid_inc_tax` decimal(7,2) DEFAULT NULL,
  `ws_net_paid_inc_ship` decimal(7,2) DEFAULT NULL,
  `ws_net_paid_inc_ship_tax` decimal(7,2) DEFAULT NULL,
  `ws_net_profit` decimal(7,2) DEFAULT NULL,
  PRIMARY KEY (`ws_item_sk`,`ws_order_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `web_sales`
--


--
-- Table structure for table `web_site`
--

DROP TABLE IF EXISTS `web_site`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `web_site` (
  `web_site_sk` int NOT NULL,
  `web_site_id` char(16) NOT NULL,
  `web_rec_start_date` date DEFAULT NULL,
  `web_rec_end_date` date DEFAULT NULL,
  `web_name` varchar(50) DEFAULT NULL,
  `web_open_date_sk` int DEFAULT NULL,
  `web_close_date_sk` int DEFAULT NULL,
  `web_class` varchar(50) DEFAULT NULL,
  `web_manager` varchar(40) DEFAULT NULL,
  `web_mkt_id` int DEFAULT NULL,
  `web_mkt_class` varchar(50) DEFAULT NULL,
  `web_mkt_desc` varchar(100) DEFAULT NULL,
  `web_market_manager` varchar(40) DEFAULT NULL,
  `web_company_id` int DEFAULT NULL,
  `web_company_name` char(50) DEFAULT NULL,
  `web_street_number` char(10) DEFAULT NULL,
  `web_street_name` varchar(60) DEFAULT NULL,
  `web_street_type` char(15) DEFAULT NULL,
  `web_suite_number` char(10) DEFAULT NULL,
  `web_city` varchar(60) DEFAULT NULL,
  `web_county` varchar(30) DEFAULT NULL,
  `web_state` char(2) DEFAULT NULL,
  `web_zip` char(10) DEFAULT NULL,
  `web_country` varchar(20) DEFAULT NULL,
  `web_gmt_offset` decimal(5,2) DEFAULT NULL,
  `web_tax_percentage` decimal(5,2) DEFAULT NULL,
  PRIMARY KEY (`web_site_sk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `web_site`
--


--
-- Dumping events for database 'diagnosis'
--

--
-- Dumping routines for database 'diagnosis'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-07-02 17:21:55

--
-- Deferred MySQL constraints and indexes for faster bulk import
--
