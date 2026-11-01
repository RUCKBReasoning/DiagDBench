-- MySQL dump 10.13  Distrib 8.4.9, for Linux (x86_64)
--
-- Host: 127.0.0.1    Database: tpch_1
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
-- Current Database: `tpch_1`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `tpch_1` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

USE `tpch_1`;


--
-- Table structure for table `customer`
--

DROP TABLE IF EXISTS `customer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `customer` (
  `c_custkey` int NOT NULL,
  `c_name` varchar(25) NOT NULL,
  `c_address` varchar(40) NOT NULL,
  `c_nationkey` int NOT NULL,
  `c_phone` char(15) NOT NULL,
  `c_acctbal` decimal(15,2) NOT NULL,
  `c_mktsegment` char(10) NOT NULL,
  `c_comment` varchar(117) NOT NULL,
  PRIMARY KEY (`c_custkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `customer`
--


--
-- Table structure for table `lineitem`
--

DROP TABLE IF EXISTS `lineitem`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `lineitem` (
  `l_orderkey` int NOT NULL,
  `l_partkey` int NOT NULL,
  `l_suppkey` int NOT NULL,
  `l_linenumber` int NOT NULL,
  `l_quantity` decimal(15,2) NOT NULL,
  `l_extendedprice` decimal(15,2) NOT NULL,
  `l_discount` decimal(15,2) NOT NULL,
  `l_tax` decimal(15,2) NOT NULL,
  `l_returnflag` char(1) NOT NULL,
  `l_linestatus` char(1) NOT NULL,
  `l_shipdate` date NOT NULL,
  `l_commitdate` date NOT NULL,
  `l_receiptdate` date NOT NULL,
  `l_shipinstruct` char(25) NOT NULL,
  `l_shipmode` char(10) NOT NULL,
  `l_comment` varchar(44) NOT NULL,
  PRIMARY KEY (`l_orderkey`,`l_linenumber`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lineitem`
--


--
-- Table structure for table `nation`
--

DROP TABLE IF EXISTS `nation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `nation` (
  `n_nationkey` int NOT NULL,
  `n_name` char(25) NOT NULL,
  `n_regionkey` int NOT NULL,
  `n_comment` varchar(152) DEFAULT NULL,
  PRIMARY KEY (`n_nationkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `nation`
--


--
-- Table structure for table `orders`
--

DROP TABLE IF EXISTS `orders`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `orders` (
  `o_orderkey` int NOT NULL,
  `o_custkey` int NOT NULL,
  `o_orderstatus` char(1) NOT NULL,
  `o_totalprice` decimal(15,2) NOT NULL,
  `o_orderdate` date NOT NULL,
  `o_orderpriority` char(15) NOT NULL,
  `o_clerk` char(15) NOT NULL,
  `o_shippriority` int NOT NULL,
  `o_comment` varchar(79) NOT NULL,
  PRIMARY KEY (`o_orderkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `orders`
--


--
-- Table structure for table `part`
--

DROP TABLE IF EXISTS `part`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `part` (
  `p_partkey` int NOT NULL,
  `p_name` varchar(55) NOT NULL,
  `p_mfgr` char(25) NOT NULL,
  `p_brand` char(10) NOT NULL,
  `p_type` varchar(25) NOT NULL,
  `p_size` int NOT NULL,
  `p_container` char(10) NOT NULL,
  `p_retailprice` decimal(15,2) NOT NULL,
  `p_comment` varchar(23) NOT NULL,
  PRIMARY KEY (`p_partkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `part`
--


--
-- Table structure for table `partsupp`
--

DROP TABLE IF EXISTS `partsupp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `partsupp` (
  `ps_partkey` int NOT NULL,
  `ps_suppkey` int NOT NULL,
  `ps_availqty` int NOT NULL,
  `ps_supplycost` decimal(15,2) NOT NULL,
  `ps_comment` varchar(199) NOT NULL,
  PRIMARY KEY (`ps_partkey`,`ps_suppkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `partsupp`
--


--
-- Table structure for table `region`
--

DROP TABLE IF EXISTS `region`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `region` (
  `r_regionkey` int NOT NULL,
  `r_name` char(25) NOT NULL,
  `r_comment` varchar(152) DEFAULT NULL,
  PRIMARY KEY (`r_regionkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `region`
--


--
-- Table structure for table `supplier`
--

DROP TABLE IF EXISTS `supplier`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `supplier` (
  `s_suppkey` int NOT NULL,
  `s_name` char(25) NOT NULL,
  `s_address` varchar(40) NOT NULL,
  `s_nationkey` int NOT NULL,
  `s_phone` char(15) NOT NULL,
  `s_acctbal` decimal(15,2) NOT NULL,
  `s_comment` varchar(101) NOT NULL,
  PRIMARY KEY (`s_suppkey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `supplier`
--


--
-- Dumping events for database 'diagnosis'
--

--
-- Dumping routines for database 'diagnosis'
--

--
-- Deferred TPCH constraints and indexes for faster bulk import
--

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-07-02 16:22:32
