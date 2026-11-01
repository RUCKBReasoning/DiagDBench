ALTER TABLE `call_center`
  ADD KEY `cc_d1` (`cc_closed_date_sk`),
  ADD KEY `cc_d2` (`cc_open_date_sk`);
ALTER TABLE `catalog_page`
  ADD KEY `cp_d1` (`cp_end_date_sk`),
  ADD KEY `cp_d2` (`cp_start_date_sk`);
ALTER TABLE `catalog_returns`
  ADD KEY `cr_a1` (`cr_refunded_addr_sk`),
  ADD KEY `cr_a2` (`cr_returning_addr_sk`),
  ADD KEY `cr_c1` (`cr_refunded_customer_sk`),
  ADD KEY `cr_c2` (`cr_returning_customer_sk`),
  ADD KEY `cr_cc` (`cr_call_center_sk`),
  ADD KEY `cr_cd1` (`cr_refunded_cdemo_sk`),
  ADD KEY `cr_cd2` (`cr_returning_cdemo_sk`),
  ADD KEY `cr_cp` (`cr_catalog_page_sk`),
  ADD KEY `cr_d1` (`cr_returned_date_sk`),
  ADD KEY `cr_hd1` (`cr_refunded_hdemo_sk`),
  ADD KEY `cr_hd2` (`cr_returning_hdemo_sk`),
  ADD KEY `cr_r` (`cr_reason_sk`),
  ADD KEY `cr_sm` (`cr_ship_mode_sk`),
  ADD KEY `cr_t` (`cr_returned_time_sk`),
  ADD KEY `cr_w2` (`cr_warehouse_sk`);
ALTER TABLE `catalog_sales`
  ADD KEY `cs_b_a` (`cs_bill_addr_sk`),
  ADD KEY `cs_b_c` (`cs_bill_customer_sk`),
  ADD KEY `cs_b_cd` (`cs_bill_cdemo_sk`),
  ADD KEY `cs_b_hd` (`cs_bill_hdemo_sk`),
  ADD KEY `cs_cc` (`cs_call_center_sk`),
  ADD KEY `cs_cp` (`cs_catalog_page_sk`),
  ADD KEY `cs_d1` (`cs_ship_date_sk`),
  ADD KEY `cs_d2` (`cs_sold_date_sk`),
  ADD KEY `cs_p` (`cs_promo_sk`),
  ADD KEY `cs_s_a` (`cs_ship_addr_sk`),
  ADD KEY `cs_s_c` (`cs_ship_customer_sk`),
  ADD KEY `cs_s_cd` (`cs_ship_cdemo_sk`),
  ADD KEY `cs_s_hd` (`cs_ship_hdemo_sk`),
  ADD KEY `cs_sm` (`cs_ship_mode_sk`),
  ADD KEY `cs_t` (`cs_sold_time_sk`),
  ADD KEY `cs_w` (`cs_warehouse_sk`);
ALTER TABLE `customer`
  ADD KEY `c_a` (`c_current_addr_sk`),
  ADD KEY `c_cd` (`c_current_cdemo_sk`),
  ADD KEY `c_fsd` (`c_first_sales_date_sk`),
  ADD KEY `c_fsd2` (`c_first_shipto_date_sk`),
  ADD KEY `c_hd` (`c_current_hdemo_sk`);
ALTER TABLE `household_demographics` ADD KEY `hd_ib` (`hd_income_band_sk`);
ALTER TABLE `inventory`
  ADD KEY `inv_i` (`inv_item_sk`),
  ADD KEY `inv_w` (`inv_warehouse_sk`);
ALTER TABLE `promotion`
  ADD KEY `p_end_date` (`p_end_date_sk`),
  ADD KEY `p_i` (`p_item_sk`),
  ADD KEY `p_start_date` (`p_start_date_sk`);
ALTER TABLE `store` ADD KEY `s_close_date` (`s_closed_date_sk`);
ALTER TABLE `store_returns`
  ADD KEY `sr_a` (`sr_addr_sk`),
  ADD KEY `sr_c` (`sr_customer_sk`),
  ADD KEY `sr_cd` (`sr_cdemo_sk`),
  ADD KEY `sr_hd` (`sr_hdemo_sk`),
  ADD KEY `sr_r` (`sr_reason_sk`),
  ADD KEY `sr_ret_d` (`sr_returned_date_sk`),
  ADD KEY `sr_s` (`sr_store_sk`),
  ADD KEY `sr_t` (`sr_return_time_sk`);
ALTER TABLE `store_sales`
  ADD KEY `ss_a` (`ss_addr_sk`),
  ADD KEY `ss_c` (`ss_customer_sk`),
  ADD KEY `ss_cd` (`ss_cdemo_sk`),
  ADD KEY `ss_d` (`ss_sold_date_sk`),
  ADD KEY `ss_hd` (`ss_hdemo_sk`),
  ADD KEY `ss_p` (`ss_promo_sk`),
  ADD KEY `ss_s` (`ss_store_sk`),
  ADD KEY `ss_t` (`ss_sold_time_sk`);
ALTER TABLE `web_page`
  ADD KEY `wp_ad` (`wp_access_date_sk`),
  ADD KEY `wp_cd` (`wp_creation_date_sk`);
ALTER TABLE `web_returns`
  ADD KEY `wr_r` (`wr_reason_sk`),
  ADD KEY `wr_ref_a` (`wr_refunded_addr_sk`),
  ADD KEY `wr_ref_c` (`wr_refunded_customer_sk`),
  ADD KEY `wr_ref_cd` (`wr_refunded_cdemo_sk`),
  ADD KEY `wr_ref_hd` (`wr_refunded_hdemo_sk`),
  ADD KEY `wr_ret_a` (`wr_returning_addr_sk`),
  ADD KEY `wr_ret_c` (`wr_returning_customer_sk`),
  ADD KEY `wr_ret_cd` (`wr_returning_cdemo_sk`),
  ADD KEY `wr_ret_d` (`wr_returned_date_sk`),
  ADD KEY `wr_ret_hd` (`wr_returning_hdemo_sk`),
  ADD KEY `wr_ret_t` (`wr_returned_time_sk`),
  ADD KEY `wr_wp` (`wr_web_page_sk`);
ALTER TABLE `web_sales`
  ADD KEY `ws_b_a` (`ws_bill_addr_sk`),
  ADD KEY `ws_b_c` (`ws_bill_customer_sk`),
  ADD KEY `ws_b_cd` (`ws_bill_cdemo_sk`),
  ADD KEY `ws_b_hd` (`ws_bill_hdemo_sk`),
  ADD KEY `ws_d2` (`ws_sold_date_sk`),
  ADD KEY `ws_p` (`ws_promo_sk`),
  ADD KEY `ws_s_a` (`ws_ship_addr_sk`),
  ADD KEY `ws_s_c` (`ws_ship_customer_sk`),
  ADD KEY `ws_s_cd` (`ws_ship_cdemo_sk`),
  ADD KEY `ws_s_d` (`ws_ship_date_sk`),
  ADD KEY `ws_s_hd` (`ws_ship_hdemo_sk`),
  ADD KEY `ws_sm` (`ws_ship_mode_sk`),
  ADD KEY `ws_t` (`ws_sold_time_sk`),
  ADD KEY `ws_w2` (`ws_warehouse_sk`),
  ADD KEY `ws_wp` (`ws_web_page_sk`),
  ADD KEY `ws_ws` (`ws_web_site_sk`);
ALTER TABLE `web_site`
  ADD KEY `web_d1` (`web_close_date_sk`),
  ADD KEY `web_d2` (`web_open_date_sk`);
ALTER TABLE `call_center`
  ADD CONSTRAINT `cc_d1` FOREIGN KEY (`cc_closed_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `cc_d2` FOREIGN KEY (`cc_open_date_sk`) REFERENCES `date_dim` (`d_date_sk`);
ALTER TABLE `catalog_page`
  ADD CONSTRAINT `cp_d1` FOREIGN KEY (`cp_end_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `cp_d2` FOREIGN KEY (`cp_start_date_sk`) REFERENCES `date_dim` (`d_date_sk`);
ALTER TABLE `catalog_returns`
  ADD CONSTRAINT `cr_a1` FOREIGN KEY (`cr_refunded_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `cr_a2` FOREIGN KEY (`cr_returning_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `cr_c1` FOREIGN KEY (`cr_refunded_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `cr_c2` FOREIGN KEY (`cr_returning_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `cr_cc` FOREIGN KEY (`cr_call_center_sk`) REFERENCES `call_center` (`cc_call_center_sk`),
  ADD CONSTRAINT `cr_cd1` FOREIGN KEY (`cr_refunded_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `cr_cd2` FOREIGN KEY (`cr_returning_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `cr_cp` FOREIGN KEY (`cr_catalog_page_sk`) REFERENCES `catalog_page` (`cp_catalog_page_sk`),
  ADD CONSTRAINT `cr_d1` FOREIGN KEY (`cr_returned_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `cr_hd1` FOREIGN KEY (`cr_refunded_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `cr_hd2` FOREIGN KEY (`cr_returning_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `cr_i` FOREIGN KEY (`cr_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `cr_r` FOREIGN KEY (`cr_reason_sk`) REFERENCES `reason` (`r_reason_sk`),
  ADD CONSTRAINT `cr_sm` FOREIGN KEY (`cr_ship_mode_sk`) REFERENCES `ship_mode` (`sm_ship_mode_sk`),
  ADD CONSTRAINT `cr_t` FOREIGN KEY (`cr_returned_time_sk`) REFERENCES `time_dim` (`t_time_sk`),
  ADD CONSTRAINT `cr_w2` FOREIGN KEY (`cr_warehouse_sk`) REFERENCES `warehouse` (`w_warehouse_sk`);
ALTER TABLE `catalog_sales`
  ADD CONSTRAINT `cs_b_a` FOREIGN KEY (`cs_bill_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `cs_b_c` FOREIGN KEY (`cs_bill_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `cs_b_cd` FOREIGN KEY (`cs_bill_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `cs_b_hd` FOREIGN KEY (`cs_bill_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `cs_cc` FOREIGN KEY (`cs_call_center_sk`) REFERENCES `call_center` (`cc_call_center_sk`),
  ADD CONSTRAINT `cs_cp` FOREIGN KEY (`cs_catalog_page_sk`) REFERENCES `catalog_page` (`cp_catalog_page_sk`),
  ADD CONSTRAINT `cs_d1` FOREIGN KEY (`cs_ship_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `cs_d2` FOREIGN KEY (`cs_sold_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `cs_i` FOREIGN KEY (`cs_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `cs_p` FOREIGN KEY (`cs_promo_sk`) REFERENCES `promotion` (`p_promo_sk`),
  ADD CONSTRAINT `cs_s_a` FOREIGN KEY (`cs_ship_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `cs_s_c` FOREIGN KEY (`cs_ship_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `cs_s_cd` FOREIGN KEY (`cs_ship_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `cs_s_hd` FOREIGN KEY (`cs_ship_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `cs_sm` FOREIGN KEY (`cs_ship_mode_sk`) REFERENCES `ship_mode` (`sm_ship_mode_sk`),
  ADD CONSTRAINT `cs_t` FOREIGN KEY (`cs_sold_time_sk`) REFERENCES `time_dim` (`t_time_sk`),
  ADD CONSTRAINT `cs_w` FOREIGN KEY (`cs_warehouse_sk`) REFERENCES `warehouse` (`w_warehouse_sk`);
ALTER TABLE `customer`
  ADD CONSTRAINT `c_a` FOREIGN KEY (`c_current_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `c_cd` FOREIGN KEY (`c_current_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `c_fsd` FOREIGN KEY (`c_first_sales_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `c_fsd2` FOREIGN KEY (`c_first_shipto_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `c_hd` FOREIGN KEY (`c_current_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`);
ALTER TABLE `household_demographics` ADD CONSTRAINT `hd_ib` FOREIGN KEY (`hd_income_band_sk`) REFERENCES `income_band` (`ib_income_band_sk`);
ALTER TABLE `inventory`
  ADD CONSTRAINT `inv_d` FOREIGN KEY (`inv_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `inv_i` FOREIGN KEY (`inv_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `inv_w` FOREIGN KEY (`inv_warehouse_sk`) REFERENCES `warehouse` (`w_warehouse_sk`);
ALTER TABLE `promotion`
  ADD CONSTRAINT `p_end_date` FOREIGN KEY (`p_end_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `p_i` FOREIGN KEY (`p_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `p_start_date` FOREIGN KEY (`p_start_date_sk`) REFERENCES `date_dim` (`d_date_sk`);
ALTER TABLE `store` ADD CONSTRAINT `s_close_date` FOREIGN KEY (`s_closed_date_sk`) REFERENCES `date_dim` (`d_date_sk`);
ALTER TABLE `store_returns`
  ADD CONSTRAINT `sr_a` FOREIGN KEY (`sr_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `sr_c` FOREIGN KEY (`sr_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `sr_cd` FOREIGN KEY (`sr_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `sr_hd` FOREIGN KEY (`sr_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `sr_i` FOREIGN KEY (`sr_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `sr_r` FOREIGN KEY (`sr_reason_sk`) REFERENCES `reason` (`r_reason_sk`),
  ADD CONSTRAINT `sr_ret_d` FOREIGN KEY (`sr_returned_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `sr_s` FOREIGN KEY (`sr_store_sk`) REFERENCES `store` (`s_store_sk`),
  ADD CONSTRAINT `sr_t` FOREIGN KEY (`sr_return_time_sk`) REFERENCES `time_dim` (`t_time_sk`);
ALTER TABLE `store_sales`
  ADD CONSTRAINT `ss_a` FOREIGN KEY (`ss_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `ss_c` FOREIGN KEY (`ss_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `ss_cd` FOREIGN KEY (`ss_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `ss_d` FOREIGN KEY (`ss_sold_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `ss_hd` FOREIGN KEY (`ss_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `ss_i` FOREIGN KEY (`ss_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `ss_p` FOREIGN KEY (`ss_promo_sk`) REFERENCES `promotion` (`p_promo_sk`),
  ADD CONSTRAINT `ss_s` FOREIGN KEY (`ss_store_sk`) REFERENCES `store` (`s_store_sk`),
  ADD CONSTRAINT `ss_t` FOREIGN KEY (`ss_sold_time_sk`) REFERENCES `time_dim` (`t_time_sk`);
ALTER TABLE `web_page`
  ADD CONSTRAINT `wp_ad` FOREIGN KEY (`wp_access_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `wp_cd` FOREIGN KEY (`wp_creation_date_sk`) REFERENCES `date_dim` (`d_date_sk`);
ALTER TABLE `web_returns`
  ADD CONSTRAINT `wr_i` FOREIGN KEY (`wr_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `wr_r` FOREIGN KEY (`wr_reason_sk`) REFERENCES `reason` (`r_reason_sk`),
  ADD CONSTRAINT `wr_ref_a` FOREIGN KEY (`wr_refunded_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `wr_ref_c` FOREIGN KEY (`wr_refunded_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `wr_ref_cd` FOREIGN KEY (`wr_refunded_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `wr_ref_hd` FOREIGN KEY (`wr_refunded_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `wr_ret_a` FOREIGN KEY (`wr_returning_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `wr_ret_c` FOREIGN KEY (`wr_returning_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `wr_ret_cd` FOREIGN KEY (`wr_returning_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `wr_ret_d` FOREIGN KEY (`wr_returned_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `wr_ret_hd` FOREIGN KEY (`wr_returning_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `wr_ret_t` FOREIGN KEY (`wr_returned_time_sk`) REFERENCES `time_dim` (`t_time_sk`),
  ADD CONSTRAINT `wr_wp` FOREIGN KEY (`wr_web_page_sk`) REFERENCES `web_page` (`wp_web_page_sk`);
ALTER TABLE `web_sales`
  ADD CONSTRAINT `ws_b_a` FOREIGN KEY (`ws_bill_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `ws_b_c` FOREIGN KEY (`ws_bill_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `ws_b_cd` FOREIGN KEY (`ws_bill_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `ws_b_hd` FOREIGN KEY (`ws_bill_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `ws_d2` FOREIGN KEY (`ws_sold_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `ws_i` FOREIGN KEY (`ws_item_sk`) REFERENCES `item` (`i_item_sk`),
  ADD CONSTRAINT `ws_p` FOREIGN KEY (`ws_promo_sk`) REFERENCES `promotion` (`p_promo_sk`),
  ADD CONSTRAINT `ws_s_a` FOREIGN KEY (`ws_ship_addr_sk`) REFERENCES `customer_address` (`ca_address_sk`),
  ADD CONSTRAINT `ws_s_c` FOREIGN KEY (`ws_ship_customer_sk`) REFERENCES `customer` (`c_customer_sk`),
  ADD CONSTRAINT `ws_s_cd` FOREIGN KEY (`ws_ship_cdemo_sk`) REFERENCES `customer_demographics` (`cd_demo_sk`),
  ADD CONSTRAINT `ws_s_d` FOREIGN KEY (`ws_ship_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `ws_s_hd` FOREIGN KEY (`ws_ship_hdemo_sk`) REFERENCES `household_demographics` (`hd_demo_sk`),
  ADD CONSTRAINT `ws_sm` FOREIGN KEY (`ws_ship_mode_sk`) REFERENCES `ship_mode` (`sm_ship_mode_sk`),
  ADD CONSTRAINT `ws_t` FOREIGN KEY (`ws_sold_time_sk`) REFERENCES `time_dim` (`t_time_sk`),
  ADD CONSTRAINT `ws_w2` FOREIGN KEY (`ws_warehouse_sk`) REFERENCES `warehouse` (`w_warehouse_sk`),
  ADD CONSTRAINT `ws_wp` FOREIGN KEY (`ws_web_page_sk`) REFERENCES `web_page` (`wp_web_page_sk`),
  ADD CONSTRAINT `ws_ws` FOREIGN KEY (`ws_web_site_sk`) REFERENCES `web_site` (`web_site_sk`);
ALTER TABLE `web_site`
  ADD CONSTRAINT `web_d1` FOREIGN KEY (`web_close_date_sk`) REFERENCES `date_dim` (`d_date_sk`),
  ADD CONSTRAINT `web_d2` FOREIGN KEY (`web_open_date_sk`) REFERENCES `date_dim` (`d_date_sk`);
