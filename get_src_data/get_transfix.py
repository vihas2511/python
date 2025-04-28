'''
Set of functions getting shipment information from Transfix/TransVisibility

Modifications:
22.06.2020 - Krystian Jedlinski - get_on_time_deta_hub_star() function modified and on_time_data_hub_select_csot_v3
             list added to fulfill requirements of new CSOT calcucations (CSOT Actual On Time %).
'''

import utils


def get_on_time_data_hub_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix, csot_v3_calc=False):
    ''' Get a DF with on_time_data_hub_star data. '''

    logging.info("Started selecting on_time_data_hub_star from {}.".format(src_db_name))
    select_fields = on_time_data_hub_select_csot_v3 if csot_v3_calc else on_time_data_hub_select
    on_time_data_hub_star_df = spark_session.table("{}.on_time_data_hub_star".format(src_db_name))\
        .selectExpr(select_fields)
    logging.info("Selecting on_time_data_hub_star from {} has finished.".format(src_db_name))

    return on_time_data_hub_star_df


def get_sambc_master(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with sambc data. '''

    logging.info("Started selecting sambc_master from {}.".format(src_db_name))
    sambc_df = spark_session.table("{}.sambc_master_lkp".format(src_db_name))\
        .select("customer_lvl3_desc", "sambc_flag")
    logging.info("Selecting sambc_master from {} has finished.".format(src_db_name))

    return sambc_df


def get_csot_bucket(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with csot_bucket data. '''

    logging.info("Started selecting csot_bucket from {}.".format(src_db_name))
    csot_bucket_df = spark_session.table("{}.csot_update_reason_lkp".format(src_db_name))\
        .select("load_id", "cust_po_num", "csot_update_reason_code", "reason_code", "aot_reason_code", "poload_id")
    logging.info("Selecting csot_bucket from {} has finished.".format(src_db_name))

    return csot_bucket_df


def get_tfs(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tfs data. '''

    logging.info("Started selecting tfs from {}.".format(src_db_name))
    tfs_df = spark_session.table("{}.tfs_technical_name_star".format(src_db_name)).selectExpr(tfs_select_expr)
    # .filter('length(shpmt_id) > 1')
    logging.info("Selecting tfs from {} has finished.".format(src_db_name))

    return tfs_df


def get_tfs_data(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tfs data. '''

    logging.info("Started selecting tfs from {}.".format(src_db_name))
    tfs_df = spark_session.table("{}.tfs_technical_name_star".format(src_db_name))\
        .select("shpmt_id", "tdcval_desc", "su_per_load_qty")
    logging.info("Selecting tfs from {} has finished.".format(src_db_name))

    return tfs_df


def get_tender_acceptance_na_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tender_acceptance_na_star data. '''

    logging.info("Started selecting tender_acceptance_na_star from {}.".format(src_db_name))
    tender_acceptance_na_star_df = spark_session.table("{}.tender_acceptance_na_star".format(src_db_name))\
        .filter('length(load_id) > 1').selectExpr(tender_acceptance_select_expr)
    logging.info("Selecting tender_acceptance_na_star from {} has finished.".format(src_db_name))

    return tender_acceptance_na_star_df


def get_on_time_arriv_shpmt_custshpmt_na_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with on_time_arriv_shpmt_custshpmt_na_star data. '''

    logging.info("Started selecting on_time_arriv_shpmt_custshpmt_na_star from {}.".format(src_db_name))
    on_time_arriv_shpmt_custshpmt_na_star_df = \
        spark_session.table("{}.on_time_arriv_shpmt_custshpmt_na_star".format(src_db_name))\
        .selectExpr(on_time_arriv_shpmt_custshpmt_select_expr).filter('length(shpmt_id) > 1')
    logging.info("Selecting on_time_arriv_shpmt_custshpmt_na_star from {} has finished.".format(src_db_name))

    return on_time_arriv_shpmt_custshpmt_na_star_df


def get_tac_tender_pg_summary(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tac_tender_pg_summary data. '''

    logging.info("Started selecting tac_tender_pg_summary from {}.".format(src_db_name))
    tac_tender_pg_summary_df = spark_session.table("{}.tac_tender_summary_star".format(src_db_name)).\
        selectExpr(tac_tender_pg_summary_select_expr)
    logging.info("Selecting tac_tender_pg_summary from {} has finished.".format(src_db_name))

    return tac_tender_pg_summary_df

def get_tac_tender_pg_summary_new_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tac_tender_pg_summary_new_star data. '''

    logging.info("Started selecting tac_tender_pg_summary_new_star from {}.".format(src_db_name))
    tac_tender_pg_summary_new_df = spark_session.table("{}.tac_tender_summary_new_star".format(src_db_name)).\
        selectExpr(tac_tender_pg_summary_new_star_select_expr)
    logging.info("Selecting tac_tender_pg_summary_new_star from {} has finished.".format(src_db_name))

    return tac_tender_pg_summary_new_df

def get_tac_tender_pg_summary_from_view(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tac_tender_pg_summary data. '''

    logging.info("Started selecting tac_tender_pg_summary from {}.".format(src_db_name))
    tac_tender_pg_summary_vw_df = spark_session.table("ap_transfix_tv_na.tac_tender_pg_summary".format(src_db_name)).\
        selectExpr(tac_tender_pg_summary_vw_select_expr)
    logging.info("Selecting tac_tender_pg_summary from {} has finished.".format(src_db_name))

    return tac_tender_pg_summary_vw_df

def get_tac_tender_pg_summary_new (
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tac_tender_pg_summary data. '''

    logging.info("Started selecting tac_tender_pg_summary_new from {}.".format(src_db_name))
    tac_tender_pg_summary_vw_df = spark_session.table("{}.tac_tender_pg_summary_new".format(src_db_name)).\
        selectExpr(tac_tender_pg_summary_new_select_expr)
    logging.info("Selecting tac_tender_pg_summary_new from {} has finished.".format(src_db_name))

    return tac_tender_pg_summary_vw_df

def get_shipping_location_na_dim(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with shipping_location_na_dim data. '''

    logging.info("Started selecting shipping_location_na_dim from {}."
                 .format(src_db_name))
    # Load shipping_location_na_dim data
    shipping_location_na_dim_sql = """
    SELECT
        state_province_code
      , loc_name
      , postal_code
      , loc_id
      , corp2_id AS origin_zone_ship_from_code
    FROM {}.shipping_location_na_dim 
    """.format(src_db_name)

    shipping_location_na_dim_df = spark_session.sql(shipping_location_na_dim_sql)

    logging.info("Selecting shipping_location_na_dim from {} has finished.".format(src_db_name))

    return shipping_location_na_dim_df


def get_leo_truck_report_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with leo_truck_report_lkp data. '''

    logging.info("Started selecting leo_truck_report_lkp from {}.".format(src_db_name))
    leo_truck_report_lkp_df = spark_session.table("{}.leo_truck_report_lkp".format(src_db_name))
    logging.info("Selecting leo_truck_report_lkp from {} has finished.".format(src_db_name))

    return leo_truck_report_lkp_df


def get_leo_vehicle_maintenance_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with leo_vehicle_maintenance_lkp data. '''

    logging.info("Started selecting leo_vehicle_maintenance_lkp from {}.".format(src_db_name))
    leo_vehicle_maintenance_lkp_df = spark_session.table("{}.leo_vehicle_maintenance_lkp".format(src_db_name))
    logging.info("Selecting leo_vehicle_maintenance_lkp from {} has finished.".format(src_db_name))

    return leo_vehicle_maintenance_lkp_df


def get_order_shipment_linkage_zvdf_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with order_shipment_linkage_zvdf_lkp data. '''

    logging.info("Started selecting order_shipment_linkage_zvdf_lkp from {}.".format(src_db_name))
    order_shipment_linkage_zvdf_lkp_df = spark_session.table("{}.order_shipment_linkage_zvdf_lkp".format(src_db_name))
    logging.info("Selecting order_shipment_linkage_zvdf_lkp from {} has finished.".format(src_db_name))

    return order_shipment_linkage_zvdf_lkp_df


def get_otd_vfr_na_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with otd_vfr_na_star data. '''

    logging.info("Started selecting otd_vfr_na_star from {}.".format(src_db_name))
    otd_vfr_na_star_df = spark_session.table("{}.otd_vfr_na_star".format(src_db_name)).selectExpr(vfr_select_expr)
    logging.info("Selecting otd_vfr_na_star from {} has finished.".format(src_db_name))

    return otd_vfr_na_star_df


def get_tac(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with TAC data. '''

    logging.info("Started selecting tac from {}.".format(src_db_name))
    tac_df = spark_session.table("{}.tac_technical_name_star".format(src_db_name)) \
        .filter('length(load_id) > 1').selectExpr(tac_select_expr)
    logging.info("Selecting tac from {} has finished.".format(src_db_name))

    return tac_df


def get_tac_lane_detail_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tac_lane_detail_star data. '''

    logging.info("Started selecting tac_lane_detail_star from {}.".format(src_db_name))
    # Load tac_lane_detail_star data
    tac_lane_detail_star_sql = """
    SELECT DISTINCT week_begin_date
    FROM {}.tac_lane_detail_star""".format(src_db_name)

    tac_lane_detail_star_df = spark_session.sql(tac_lane_detail_star_sql)

    logging.info("Selecting tac_lane_detail_star from {} has finished.".format(src_db_name))

    return tac_lane_detail_star_df


def get_csot_data(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with csot data. '''

    logging.info("Started selecting csot from {}.".format(src_db_name))
    csot_df = spark_session.table("{}.csot_star".format(src_db_name))
    logging.info("Selecting csot from {} has finished.".format(src_db_name))

    return csot_df

def get_monster_view_data(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with monster data. '''

    logging.info("Started selecting monster view from {}.".format(src_db_name))
    monster_view_df = spark_session.table("ap_transfix_tv_na.monster_report_vw".format(src_db_name)).selectExpr(monster_vw_select_expr)
    logging.info("Selecting monster view from {} has finished.".format(src_db_name))

    return monster_view_df

def get_lot_view_data(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with lot data. '''

    logging.info("Started selecting lot view from {}.".format(src_db_name))
    lot_view_df = spark_session.table("ap_transfix_tv_na.lot".format(src_db_name)).selectExpr(lot_vw_select_expr)
    logging.info("Selecting lot view from {} has finished.".format(src_db_name))

    return lot_view_df

def get_vfr_agg_data(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with lot data. '''

    logging.info("Started selecting vfr_load_agg_star from {}.".format(src_db_name))
    vfr_agg_df = spark_session.table("{}.vfr_load_agg_star".format(src_db_name)).selectExpr(vfr_vw_select_expr)
    logging.info("Selecting vfr_load_agg_star from {} has finished.".format(src_db_name))

    return vfr_agg_df


def get_origin_gbu_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with origin_gbu_lkp data. '''

    logging.info("Started selecting origin_gbu_lkp from {}.".format(src_db_name))
    origin_gbu_lkp_df = spark_session.table("{}.origin_gbu_lkp".format(src_db_name))\
        .select("origin_id", "parent_loc_code", "gbu_code")
    logging.info("Selecting origin_gbu_lkp from {} has finished.".format(src_db_name))

    return origin_gbu_lkp_df


def get_destination_channel_customer_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with destination_channel_customer_lkp data. '''

    logging.info("Started selecting destination_channel_customer_lkp from {}.".format(src_db_name))
    destination_channel_customer_lkp_df = spark_session.table("{}.destination_channel_customer_lkp".format(src_db_name))\
        .select("customer_name", "channel_name", "dest_sold_to_name", "level_name", "country_code")
    logging.info("Selecting destination_channel_customer_lkp from {} has finished.".format(src_db_name))

    return destination_channel_customer_lkp_df


def get_ship_to_pgp_flag_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with ship_to_pgp_flag_lkp data. '''

    logging.info("Started selecting ship_to_pgp_flag_lkp from {}.".format(src_db_name))
    ship_to_pgp_flag_lkp_df = spark_session.table("{}.ship_to_pgp_flag_lkp".format(src_db_name))\
        .select("ship_to_num", "pgp_flag")
    logging.info("Selecting ship_to_pgp_flag_lkp from {} has finished.".format(src_db_name))

    return ship_to_pgp_flag_lkp_df


def get_on_time_codes_aot_reason_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with on_time_codes_aot_reason_lkp data. '''

    logging.info("Started selecting on_time_codes_aot_reason_lkp from {}.".format(src_db_name))
    on_time_codes_aot_reason_lkp_df = spark_session.table("{}.on_time_codes_aot_reason_lkp".format(src_db_name))\
        .select("definition_name", "cause_desc")
    logging.info("Selecting on_time_codes_aot_reason_lkp from {} has finished.".format(src_db_name))

    return on_time_codes_aot_reason_lkp_df


def get_tdcval_md(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tac_lane_detail_star data. '''

    logging.info("Started selecting tdcval_na_dim from {}.".format(src_db_name))
    tdcval_md_df = spark_session.table("{}.tdcval_na_dim".format(src_db_name)).selectExpr(tdcval_select_expr)
    logging.info("Selecting tdcval_na_dim from {} has finished.".format(src_db_name))

    return tdcval_md_df


#def get_actual_ship_time_lkp(
#        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
#    ''' Get a DF with actual_ship_time_lkp data. '''
#
#    logging.info("Started selecting actual_ship_time_lkp from {}.".format(src_db_name))
#    actual_ship_time_df = spark_session.table("{}.actual_ship_time_lkp".format(src_db_name))\
#        .select("load_id", "actual_ship_datetm")
#    logging.info("Selecting actual_ship_time_lkp from {} has finished.".format(src_db_name))
#
#    return actual_ship_time_df


def get_tms_unload_method_dest_zone_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with actual_ship_time_lkp data. '''

    logging.info("Started selecting tms_unload_method_dest_zone_lkp from {}.".format(src_db_name))
    tms_unload_method_df = spark_session.table("{}.tms_unload_method_dest_zone_lkp".format(src_db_name))\
        .select("load_id", "dest_zone_code", "actual_unload_method_val")
    logging.info("Selecting tms_unload_method_dest_zone_lkp from {} has finished.".format(src_db_name))

    return tms_unload_method_df


def get_vfr_us_ca_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with csot data. '''

    logging.info("Started selecting vfr_us_ca from {}.".format(src_db_name))
    vfr_us_ca_df = spark_session.table("{}.vfr_us_ca".format(src_db_name)).selectExpr(vfr_us_ca_select_expr)
    logging.info("Selecting vfr_us_ca from {} has finished.".format(src_db_name))

    return vfr_us_ca_df


def get_vfr_data_hub_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with vfr_data_hub_star data. '''

    logging.info("Started selecting vfr_data_hub_star from {}.".format(src_db_name))
    vfr_data_hub_star_df = spark_session.table("{}.vfr_data_hub_star".format(src_db_name)).selectExpr(vfr_data_hub_select_exp)
    logging.info("Selecting vfr_data_hub_star from {} has finished.".format(src_db_name))

    return vfr_data_hub_star_df


def get_vfr_data_hub_optny_bucket_lkp(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with vfr_data_hub_optny_bucket_lkp data. '''

    logging.info("Started selecting vfr_data_hub_optny_bucket_lkp from {}.".format(src_db_name))
    vfr_data_hub_optny_bucket_lkp_df = spark_session.table("{}.vfr_data_hub_optny_bucket_lkp".format(src_db_name))\
        .select("load_id", "gbu_code", "tdcval_code", "optny_bucket_desc", "optny_bucket_val")
    logging.info("Selecting vfr_data_hub_optny_bucket_lkp from {} has finished.".format(src_db_name))

    return vfr_data_hub_optny_bucket_lkp_df


def get_freight_stats_na_merged_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with otd_vfr_na_star data. '''

    logging.info("Started selecting freight_stats_na_merged_star from {}.".format(src_db_name))
    freight_stats_na_merged_star_df = spark_session.table("{}.freight_stats_na_merged_star".format(src_db_name))\
        .select("shpmt_id", "frt_type_code")
    logging.info("Selecting freight_stats_na_merged_star from {} has finished.".format(src_db_name))

    return freight_stats_na_merged_star_df


def get_latest_tariffs(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with actual_ship_time_lkp data. '''

    logging.info("Started selecting operational_tariff_star from {}.".format(src_db_name))

    tariff_sql = """
        SELECT
            lane_origin_zone_code, lane_dstn_zone_id, origin_corp_code, dest_zip_code, dest_loc_desc, dest_corp_code, 
            tariff_id, carrier_id, carrier_name, charge_desc, mile_contract_rate, base_charge_amt, min_charge_amt, 
            rate_eff_date, rate_exp_date, alloc_type_code, alloc_profile_val, award_rate, dlvry_schedule_code, 
            equip_type_code, status_code, tariff_desc, max_no_of_shpmt_cnt, cust_id, cust_desc, report_date, rate_code, 
            from_unixtime(unix_timestamp()) AS today_date, CAST(rate_exp_date AS TIMESTAMP) AS dt_rate_exp_date, 
            CAST(rate_eff_date as TIMESTAMP) AS dt_rate_effective_date, service_code, service_desc,
            DATEDIFF(from_unixtime(unix_timestamp()), CAST(rate_eff_date as TIMESTAMP)) AS datediff_effective
        FROM {}.operational_tariff_star AS tar
            JOIN  ( SELECT MAX(report_date) as max_report_date FROM {}.operational_tariff_star 
                where report_date like '2%') AS dt
                ON tar.report_date = dt.max_report_date
        WHERE status_code = "ACTIVE" AND CAST(rate_exp_date as TIMESTAMP) >= from_unixtime(unix_timestamp())
    """.format(src_db_name, src_db_name)

    tariff_df = spark_session.sql(tariff_sql)
    logging.info("Selecting operational_tariff_star from {} has finished.".format(src_db_name))

    return tariff_df

def get_tfs_subsector_cost_star(
        logging, spark_session, src_db_name, target_db_name, staging_location, debug_mode_ind, debug_postfix):
    ''' Get a DF with tfs_subsector_cost_star data. '''

    logging.info("Started selecting tfs_subsector_cost_star from {}.".format(src_db_name))
    tfs_subsector_cost_star_df = spark_session.table("{}.tfs_subsector_cost_star".format(src_db_name))\
        .select("load_id", "distance_per_load_num_qty", "step_factor", "su_per_load_qty", "total_cost_amt",
                "subsector_desc")
    logging.info("Selecting tfs_subsector_cost_star from {} has finished.".format(src_db_name))

    return tfs_subsector_cost_star_df


def get_origin_sf_dict(logging, spark_session):
    ''' Get a DF with shipping_location_na_dim data. '''

    logging.info("Started selecting origin_sf dict values")

    origin_sf_dict_sql = """
    SELECT 'SF_AUBURN ME' as origin_sf_dict
    UNION ALL
    SELECT 'SF_BELLEVILLE' as origin_sf_dict
    UNION ALL
    SELECT 'SF_BOSTON' as origin_sf_dict
    UNION ALL
    SELECT 'SF_BROCKVILLE' as origin_sf_dict
    UNION ALL
    SELECT 'SF_KC(SOAP_PT)' as origin_sf_dict
    UNION ALL
    SELECT 'SF_ST_BERNARD' as origin_sf_dict
    UNION ALL
    SELECT 'SF_VC_GUNT_HUETL' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_31_MRTTA' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_6I_KOREX' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_6J_KIK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_AK_ANDRS' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_APAK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_BD_BSTST' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_BK_RMDBK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_BO_ANDRSN' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_CHM_PCK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_CNBRR' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_DG_OLVRC' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_FK_FTZPK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_HA_HAVPAK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_HI_HVPKI' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_IF_ACUPC' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_KE_KLEEN' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_KIK_DNVL' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_KIK_E5' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_KLEEN_TEST' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_L4_CCARE' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_LI_LSEMB' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_LR_LANTR' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_MF_MRTTA' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_MY_MVPPR' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_N1_NLSN' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_P8_PRMPK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QB_S&V' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QH_TRILL' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QLY_V3' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QN_UNIPAK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QR_FAREVA' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QV_BESTCO' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_QW_LFE_SCI' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_RP_MULTIPK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_S3_OUTLK' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_SI_SOFIDEL' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_TB_TRS' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_TH_THCHM' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_TK_THBNT' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_TL_TRITL' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_UF_UNVSL' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_VJ_CREMER' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_YQ_INTRCN' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_ZA_PERG' as origin_sf_dict
    UNION ALL
    SELECT 'SF_CM_ZI_ALPHA' as origin_sf_dict
    """

    origin_sf_dict_df = spark_session.sql(origin_sf_dict_sql)

    logging.info("Selecting origin_sf dictonary has finished.")

    return origin_sf_dict_df


# Variables with list of selected columns
on_time_data_hub_select_csot_v3 = \
    ["load_id", "profl_method_code", "first_dlvry_appt_date", "first_dlvry_appt_datetm", "request_dlvry_to_date",
     "request_dlvry_to_datetm", "last_dlvry_appt_date", "last_dlvry_appt_datetm", "orig_request_dlvry_to_date",
     "orig_request_dlvry_to_datetm", "actual_shpmt_end_date", "actual_arrival_datetm", "pg_order_num", "customer_desc",
     "cust_po_num", "ship_to_party_code", "ship_to_party_desc", "csot_failure_reason_bucket_name",
     "csot_failure_reason_bucket_updated_name", "on_time_src_code", "country_to_code", "sold_to_party_desc",
     "true_frt_type_desc", "lot_delay_reason_code"]

on_time_data_hub_select = \
    ["pg_order_num", "load_id", "cust_po_num", "ship_point_code", "ship_to_party_code", "ship_to_party_desc",
     "sold_to_party_desc", "request_dlvry_from_date", "request_dlvry_from_datetm", "request_dlvry_to_date",
     "request_dlvry_to_datetm", "orig_request_dlvry_from_tmstp", "orig_request_dlvry_to_tmstp", "actual_arrival_datetm",
     "carr_num", "carr_desc", "origin_zone_code", "origin_zone_ship_from_code", "load_method_num", "transit_mode_name",
     "order_create_date", "order_create_datetm", "schedule_date", "schedule_datetm", "tender_date", "tender_datetm",
     "first_dlvry_appt_date", "first_dlvry_appt_datetm", "last_dlvry_appt_date", "last_dlvry_appt_datetm",
     "final_lrdt_date", "final_lrdt_datetm", "actual_load_end_date", "actual_load_end_datetm", "actual_ship_date",
     "actual_ship_datetm", "csot_failure_reason_bucket_name", "csot_failure_reason_bucket_updated_name", "measrbl_flag",
     "profl_method_code", "dest_city_name", "dest_state_code", "dest_postal_code", "country_to_code",
     "actual_ship_month_num", "ship_week_num", "cause_code", "on_time_src_code", "on_time_src_desc", "true_fa_flag",
     "frt_type_desc", "sales_org_code", "sd_doc_item_overall_process_status_val", "multi_stop_num",
     "actual_service_tms_code", "lot_cust_failure_cnt", "pg_failure_cnt", "carr_failure_cnt", "others_failure_cnt",
     "tolrnc_sot_val", "lot_delay_reason_code", "lot_delay_reason_code_desc", "gbu_code", "ship_cond_code",
     "lot_ontime_status_last_appt_val", "lot_exception_categ_val", "primary_carr_flag", "otd_cnt",
     "actual_shpmt_end_date", "tat_late_counter_val", "trnsp_stage_num", "event_datetm", "actual_dlvry_tmstp",
     "first_appt_dlvry_tmstp", "ship_point_desc", "plan_shpmt_start_date", "plant_code", "ship_cond_desc",
     "sold_to_party_code", "trans_plan_point_code", "plan_shpmt_start_datetm", "cpu_for_hire_desc", "change_type_code",
     "drop_live_ind_code", "child_shpmt_num", "user_tid_val", "lot_exception_categ_desc", "change_type_desc",
     "freight_auction_flag", "parent_shpmt_flag", "dest_ship_from_code", "dest_zone_code", "ship_point_at_point_of_dprtr_code",
     "iot_on_time_load_id", "true_frt_type_desc", "load_builder_prty_val", "origin_tac_zone_code", "customer_desc",
     "first_tendered_rdd_from_datetm", "first_tendered_rdd_to_datetm", "drop_live_ind_desc"]

tfs_select_expr = \
    ["shpmt_id", "freight_cost_charge_code", "carr_desc", "total_trans_cost_usd_amt", "adjmt_cost_usd_amt",
     "contract_cost_usd_amt", "post_charge_cost_usd_amt", "spot_cost_usd_amt", "misc_cost_usd_amt",
     "weight_per_load_qty", "volume_per_load_qty", "actual_gi_date", "charge_code AS charge_reason_code", "dlvry_id",
     "ship_from_region_code AS region_from_code", "country_to_code", "country_to_desc_name AS country_to_desc",
     "ship_from_region_desc", "goods_receipt_post_date", "trans_plan_point_code",
     "tms_charge_kind_desc AS charge_kind_desc", "carr_id", "freight_type_val", "distance_per_load_qty", "hist_data",
     "accrual_cost_usd_amt", "step_per_load_rate", "flow_reason_val", "tms_freight_charge_desc", "freight_charge2_desc",
     "tms_charge_lvl_desc", "tfs_origin_zone_name", "tdcval_code", "su_per_load_qty", "freight_auction_val"]

tender_acceptance_select_expr = ["load_id", "ship_to_party_id"]

on_time_arriv_shpmt_custshpmt_select_expr = \
    ["event_datetm", "shpmt_num AS shpmt_id", "lot_delay_reasn_code AS lot_delay_reason_code",
     "pg_ordr_num AS pg_order_num", "cust_po_num", "actl_ship_month_num AS actual_ship_month_num",
     "ship_wk_num AS ship_week_num", "ordr_created_date AS order_create_date",
     "ordr_created_datetm AS order_create_datetm", "cust_grp_04_num AS cust_group_04_num",
     "ship_point_at_point_of_deprtre_code AS ship_point_at_point_of_dprtr_code", "dstnc_qty AS distance_qty",
     "sales_doc_type_code", "final_lrdt_date", "actl_loading_end_date AS actual_load_end_date",
     "actl_ship_date AS actual_ship_date", "plnd_shpmt_start_date AS plan_shpmt_start_date",
     "actl_shpmt_end_date AS actual_shpmt_end_date", "plnd_shpmt_end_date AS plan_shpmt_end_date", "leg_ind_val",
     "dstnc_uom AS distance_uom", "overall_prcsg_sttus_of_the_sd_doc_item_val AS sd_doc_item_overall_process_status_val",
     "plant_code", "plant_code_desc", "reqestd_dlvry_from_date AS request_dlvry_from_date", "sales_org_code",
     "sales_org_desc", "ship_type_code", "ship_condtns_code AS ship_cond_code", "ship_condtns_desc AS ship_cond_desc",
     "ship_point_code", "ship_point_desc", "ship_to_party_code", "ship_to_party_desc", "sold_to_party_code",
     "sold_to_party_desc", "storg_loc_code", "trans_plan_point_code", "carr_num", "carr_desc",
     "actl_loading_end_datetm AS actual_load_end_datetm", "actl_shpmt_start_datetm",
     "actl_shpmt_end_aot_datetm AS actual_shpmt_end_aot_datetm", "final_lrdt_datetm",
     "plnd_shpmt_start_datetm AS plan_shpmt_start_datetm", "plnd_shpmt_end_aot_datetm AS plan_shpmt_end_aot_datetm",
     "emerg_resp_ind AS emrgncy_resp_ind", "cmpgn_header_code AS campaign_header_code",
     "reqestd_del_to_date AS request_dlvry_to_date", "cust_lead_time_hr_qty AS cust_lead_time_hour_qty",
     "orig_reqst_dlvry_from_date AS orig_request_dlvry_from_date", "orig_reqst_dlvry_to_date AS orig_request_dlvry_to_date",
     "case_count AS case_cnt", "extrn_vendr_code AS external_vendor_code", "extrn_po_type_code AS external_po_type_code",
     "first_dlvry_appt_date", "first_dlvry_appt_datetm", "csot_failure_reasn_bucket_name AS csot_failure_reason_bucket_name",
     "walmart_event_id", "last_dlvry_appt_date", "last_dlvry_appt_datetm", "measure_applc_ind AS measrbl_flag",
     "message_code", "orig_reqst_dlvry_from_datetm AS orig_request_dlvry_from_datetm",
     "orig_reqst_dlvry_to_datetm AS orig_request_dlvry_to_datetm", "ordr_plan_hr_qty AS order_plan_hour_qty",
     "orign_postl_code AS origin_postal_code", "ordr_prcsg_hr_qty AS order_process_hour_qty",
     "orign_street_address_desc AS origin_street_address_desc", "orign_state_code AS origin_state_code",
     "orign_tac_zone_code AS origin_tac_zone_code", "early_tlrnc_val AS early_tolrnc_val", "goal_pct_val",
     "late_tlrnc_val AS late_tolrnc_val", "profl_methd_code AS profl_method_code", "prmtn_ind AS promo_ind", "pro_num",
     "reqst_dlvry_from_datetm AS request_dlvry_from_datetm", "rad_mabd_time_qty", "sched_date AS schedule_date",
     "sched_datetm AS schedule_datetm", "serv_type_name AS service_type_name", "ship_hr_qty AS ship_hour_qty",
     "std_lead_hr_qty AS std_lead_hour_qty", "tender_date", "tender_datetm", "load_methd_num AS load_method_num",
     "transit_hr_qty AS transit_hour_qty", "actl_arriv_datetm AS actual_arrival_datetm", "load_ready_datetm",
     "plnd_shpmt_end_datetm AS plan_shpmt_end_datetm", "reqst_dlvry_to_datetm AS request_dlvry_to_datetm",
     "ext_cnsldtn_num", "cpu_for_hire_desc", "tranx_type_code", "shpmt_and_stage_aot_val",
     "chng_type_code AS change_type_code", "cntry_to_code AS country_to_code", "frt_auction_code", "frt_type_code",
     "aot_ontime_sttus_last_appt_val AS aot_ontime_status_last_appt_val",
     "lot_ontime_sttus_last_appt_val AS lot_ontime_status_last_appt_val", "drop_live_ind_code",
     "lot_cust_failure_count AS lot_cust_failure_cnt", "child_shpmt_num", "orign_zone_code AS origin_zone_code",
     "carr_defect_count AS carr_defect_cnt", "cust_defect_count AS cust_defect_cnt", "late_count AS late_cnt",
     "early_and_ontime_count AS early_and_ontime_cnt", "otd_count AS otd_cnt", "others_defect_count AS others_defect_cnt",
     "pg_defect_count AS pg_defect_cnt", "on_time_src_code", "on_time_src_desc", "serv_tms_code AS service_tms_code",
     "tlrnc_aot_val AS tolrnc_aot_val", "tlrnc_sot_val AS tolrnc_sot_val", "tat_late_counter_val",
     "carr_failure_count AS carr_failure_cnt", "cust_issue_count AS cust_issue_cnt",
     "others_failure_count AS others_failure_cnt", "pg_failure_count AS pg_failure_cnt", "user_tid_val", "gbu_code",
     "transit_mode_name", "loc_type_name", "dstnc_between_stops_qty AS distance_between_stops_qty",
     "hazmat_ind AS hazmat_val", "load_builder_prty_val", "load_ship_cond_val", "dwell_minut_qty", "shpmt_rate_name",
     "shpmt_opertnl_sttus_name", "cause_code", "dest_city_name", "dest_postl_code AS dest_postal_code",
     "dest_street_address_desc", "dest_state_code", "dev_reasn_code AS deviation_reason_code",
     "ship_to_cntry_code AS ship_to_country_code", "lot_exptn_categ_val AS lot_exception_categ_val",
     "only_cpu_defect_val", "deprtre_cntry_code AS dprtr_country_code",
     "lot_delay_reasn_code_desc AS lot_delay_reason_code_desc", "lot_exptn_categ_desc AS lot_exception_categ_desc",
     "chng_type_desc AS change_type_desc", "drop_live_ind_desc", "sot_aot_reasn_code_desc",
     "dest_zone_go_name", "origin_zone_name", "origin_loc_code", "dest_loc_code", "tfts_load_tmstp",
     "load_from_file AS load_from_file_name", "bd_mod_tmstp AS row_modify_tmstp", "trnsp_stage_num", "first_tendered_rdd_from",
     "first_tendered_rdd_to"]

tac_tender_pg_summary_select_expr = \
    ["lane_name", "week_year_val", "origin_zone_ship_from_code", "dest_ship_from_code", "carr_desc",
     "avg_award_weekly_vol_qty", "country_from_code", "country_to_code", "freight_auction_flag", "week_begin_date",
     "mon_accept_cnt", "mon_total_cnt", "thu_accept_cnt", "thu_total_cnt",
     "fri_accept_cnt", "fri_total_cnt", "sun_accept_cnt", "sun_total_cnt",
     "wed_accept_cnt", "wed_total_cnt", "tue_accept_cnt", "tue_total_cnt",
     "sat_accept_cnt", "sat_total_cnt", "accept_cnt", "total_cnt", "reject_cnt",
     "accept_pct", "reject_pct", "expct_vol_val", "reject_below_award_val", "weekly_carr_rate",
     "customer_desc", "customer_code", "customer_lvl3_desc", "customer_lvl5_desc",
     "customer_lvl6_desc", "customer_lvl12_desc", "customer_spcfc_lane_name", "actual_carr_trans_cost_amt",
     "linehaul_cost_amt", "incrmtl_freight_auction_cost_amt", "cnc_carr_mix_cost_amt", "unsource_cost_amt",
     "fuel_cost_amt", "acsrl_cost_amt", "forward_agent_id", "service_tms_code", "sold_to_party_id", "ship_cond_val",
     "primary_carr_flag", "month_type_val", "cal_year_num", "month_date", "week_num", "region_code", "dest_postal_code"]

tac_tender_pg_summary_new_star_select_expr = \
    ["lane_detail_name AS lane_name", "week_year_val", "origin_zone_ship_from_code", "dest_ship_from_code", "carr_desc",
     "avg_award_weekly_vol_qty", "country_from_code", "country_to_code", "freight_auction_flag", "week_begin_date",
     "mon_accept_cnt", "mon_total_cnt", "thu_accept_cnt", "thu_total_cnt",
     "fri_accept_cnt", "fri_total_cnt", "sun_accept_cnt", "sun_total_cnt",
     "wed_accept_cnt", "wed_total_cnt", "tue_accept_cnt", "tue_total_cnt",
     "sat_accept_cnt", "sat_total_cnt", "accept_cnt", "total_cnt", "reject_cnt",
     "accept_pct", "reject_pct", "expct_vol_val", "reject_below_award_val", "weekly_carr_rate",
     "customer_desc", "customer_code", "customer_lvl3_desc", "customer_lvl5_desc",
     "customer_lvl6_desc", "customer_lvl12_desc", "customer_spcfc_lane_name", "actual_carr_trans_cost_amt",
     "linehaul_cost_amt", "incrmtl_freight_auction_cost_amt", "cnc_carr_mix_cost_amt", "unsource_cost_amt",
     "fuel_cost_amt", "acsrl_cost_amt", "forward_agent_id", "service_tms_code", "sold_to_party_id", "ship_cond_val",
     "primary_carr_flag", "month_type_val", "cal_year_num", "month_date", "week_num", "region_code", "dest_postal_code"]

vfr_select_expr = \
    ["dlvry_id", "shpmt_id", "dlvry_item_num", "trnsp_stage_num AS trans_stage_num",
     "actl_goods_issue_date AS actual_goods_issue_date", "shpmt_start_date", "vehic_type_code AS vehicle_type_code",
     "trans_plan_point_code", "frt_type_code AS vfr_freight_type_code", "ship_point_code", "ship_point_desc",
     "ship_to_id AS ship_to_party_id", "ship_to_party_desc", "sold_to_party_id", "sold_to_party_desc",
     "ship_point_cntry_to_code AS ship_point_country_to_code",
     "ship_point_at_point_deprtre_code AS ship_point_of_dprtr_code", "ship_point_at_dest_code AS ship_point_dest_code",
     "deprtre_point_code AS dprtr_point_code", "deprtre_point_cust_id AS dprtr_point_customer_id",
     "dest_point_cust_id AS dest_point_customer_id", "dest_node_val", "stage_dest_point_id",
     "stage_deprtre_point_code AS stage_dprtr_point_code", "shpmt_type_code", "leg_ind_code", "gbu_code",
     "pllt_qty AS pallet_qty", "gross_wght_qty AS gross_weight_qty", "net_wght_qty AS net_weight_qty", "gross_vol_qty",
     "net_vol_qty", "max_vol_tms_qty AS max_vol_trans_mgmt_sys_qty", "max_wght_qty AS max_weight_qty",
     "max_pllts_tms_qty AS max_pallet_tms_trans_type_qty", "dstnc_qty AS distance_qty",
     "dstnc_by_shpmt_vfr_qty AS distance_shpmt_vfr_qty", "dstnc_uom AS distance_uom",
     "gross_vol_fill_rate_wgt_by_dstnc_qty AS gross_vol_fill_rate_weight_by_distance_qty",
     "gross_wght_fill_rate_wgt_by_dstnc_qty AS gross_weight_fill_rate_weight_by_distance_qty",
     "net_vol_fill_rate_wgt_by_dstnc_qty AS net_vol_fill_rate_weight_by_distance_qty",
     "net_wght_fill_rate_wgt_by_dstnc_qty AS net_weight_fill_rate_weight_by_distance_qty",
     "thrcl_pllts_qty AS theortc_pallet_qty", "floor_positns_qty AS floor_position_qty",
     "table_uom", "fiscal_year_num", "fiscal_year_period_num AS fiscal_year_perd_num", "fiscal_year_variant_code",
     "ship_cond_val", "ship_cond_desc", "plant_code", "plant_desc AS plant_code_desc", "ship_site_gbu_name",
     "tdc_val_code AS tdcval_code", "oblb_sttus_code AS oblb_status_code", "extrn_id AS external_id",
     "oblb_gross_vol_as_plnd_qty AS oblb_gross_vol_plan_qty", "oblb_net_vol_as_plnd_qty AS oblb_net_vol_plan_qty",
     "oblb_gross_wght_as_plnd_qty AS oblb_gross_weight_plan_qty",
     "oblb_net_wght_as_plnd_qty AS oblb_net_weight_plan_qty",
     "oblb_gross_wght_as_shipped_qty AS oblb_gross_weight_shipped_qty",
     "oblb_gross_vol_as_shipped_qty AS oblb_gross_vol_shipped_qty",
     "oblb_floor_positns_as_plnd_qty AS oblb_floor_position_plan_qty",
     "plnd_gross_vol_fill_rate AS oblb_gross_vol_fill_rate_plan_qty",
     "plnd_gross_wght_fill_rate AS oblb_net_weight_fill_rate_plan_qty",
     "plnd_net_wght_fill_rate AS oblb_gross_weight_fill_rate_plan_qty",
     "plnd_net_vol_fill_rate AS oblb_net_vol_fill_rate_plan_qty",
     "shipped_gross_vol_fill_rate AS oblb_gross_vol_fill_rate_shipped_qty",
     "shipped_gross_wght_fill_rate AS oblb_gross_weight_fill_rate_shipped_qty",
     "plnd_floor_postn_fill_rate AS floor_position_fill_rate_plan_qty",
     "vol_fill_rate_above_percent_100_ind AS vol_fill_above_100pct_flag",
     "wght_fill_rate_above_percent_100_ind AS weight_fill_above_100pct_flag",
     "vfr_not_aggrgtd_dstnc_qty AS vfr_not_agg_distance_qty", "dstnc_multi_stop_qty AS distance_multi_stop_qty",
     "vehic_fill_rate_key_val AS vehicle_fill_rate_id", "ts_orig_exctd_checkin_date AS trans_origin_exectn_checkin_date",
     "ts_orig_plnd_checkin_date AS trans_origin_plan_checkin_date",
     "ts_plnd_shpmt_start_date AS trans_plan_shpmt_start_date",
     "ts_dest_exctd_shpmt_end_date AS trans_dest_exectn_shpmt_end_date",
     "ts_dest_plnd_shpmt_end_date AS trans_dest_plan_shpmt_end_date",
     "ts_dest_reqestd_shpmt_end_date AS trans_dest_request_dlvry_date",
     "actl_trnsp_start_time_val AS actual_trans_start_time_val", "recv_site_code AS recvng_site_code",
     "recv_live_or_drop_code AS recvng_live_drop_code", "sending_site_code AS send_site_code", "stage_seq_num",
     "pre_load_type_code", "cust_id AS customer_id", "cust_desc AS customer_desc", "carr_id", "carr_desc",
     "matl_val AS material_doc_num", "ordr_num_val AS order_num", "deflt_ship_cond_code AS default_ship_cond_code",
     "cntry_from_code AS country_from_code", "cntry_from_desc AS country_from_desc", "cntry_to_code AS country_to_code",
     "cntry_to_desc AS country_to_name", "flex_truck_ordr_desc AS flex_truck_order_desc", "wght_uom AS weight_uom",
     "vol_uom", "actl_cost_amt AS actual_cost_amt", "acutal_cost_crncy_code AS actual_cost_currency_code", "tdcval_desc",
     "gbu_desc", "tfts_load_tmstp", "load_from_file AS load_from_file_name", "bd_mod_tmstp AS vfr_last_update_utc_tmstp",
     "actual_trans_start_time_val AS actual_ship_datetm"]

tac_select_expr = \
    ["load_id", "origin_zone_ship_from_code", "origin_loc_id", "dest_ship_from_code", "dest_zone_code",
     "ship_to_party_id", "forward_agent_id", "carr_desc", "service_tms_code", "carr_mode_code", "carr_mode_desc",
     "tender_event_type_code", "tender_reason_code", "tender_date", "tender_datetm", "tender_event_datetm",
     "actual_goods_issue_date", "tariff_id", "schedule_code", "tender_first_carr_desc", "tender_reason_code_desc",
     "avg_award_weekly_vol_qty", "actual_ship_week_day_name", "ship_cond_val", "postal_code", "final_stop_postal_code",
     "country_from_code", "country_to_code", "freight_auction_flag", "freight_type_code", "customer_code",
     "actual_carr_trans_cost_amt", "linehaul_cost_amt", "incrmtl_freight_auction_cost_amt", "cnc_carr_mix_cost_amt",
     "unsource_cost_amt", "fuel_cost_amt", "acsrl_cost_amt", "applnc_subsector_step_cnt",
     "baby_care_subsector_step_cnt", "chemical_subsector_step_cnt", "fabric_subsector_step_cnt",
     "family_subsector_step_cnt", "fem_subsector_step_cnt", "hair_subsector_step_cnt", "home_subsector_step_cnt",
     "oral_subsector_step_cnt", "phc_subsector_step_cnt", "shave_subsector_step_cnt", "skin_subsector_cnt",
     "other_subsector_cnt", "customer_desc", "customer_lvl1_code", "customer_lvl1_desc", "customer_lvl2_code",
     "customer_lvl2_desc", "customer_lvl3_code", "customer_lvl3_desc", "customer_lvl4_code", "customer_lvl4_desc",
     "customer_lvl5_code", "customer_lvl5_desc", "customer_lvl6_code", "customer_lvl6_desc", "customer_lvl7_code",
     "customer_lvl7_desc", "customer_lvl8_code", "customer_lvl8_desc", "customer_lvl9_code", "customer_lvl9_desc",
     "customer_lvl10_code", "customer_lvl10_desc", "customer_lvl11_code", "customer_lvl11_desc", "customer_lvl12_code",
     "customer_lvl12_desc", "origin_zone_code", "daily_award_qty"]

tdcval_select_expr = ["tdcval_id AS tdcval_code", "categ_name AS category"]

tariff_select_expr = \
    ["lane_origin_zone_code", "lane_dstn_zone_id", "origin_corp_code", "dest_zip_code", "dest_loc_desc",
     "dest_corp_code", "tariff_id", "carrier_id", "carrier_name", "charge_desc", "mile_contract_rate",
     "base_charge_amt", "min_charge_amt", "rate_eff_date", "rate_exp_date", "alloc_type_code", "alloc_profile_val",
     "award_rate", "dlvry_schedule_code", "equip_type_code", "status_code", "tariff_desc", "max_no_of_shpmt_cnt",
     "cust_id", "cust_desc", "report_date"]

vfr_us_ca_select_expr = \
    ["`Actual Ship Date` AS shpmt_start_date", "`GBU` AS gbu_code", "`TDCVal` AS tdcval_code",
     "`Carrier ID` AS carr_id", "`Carrier Description` AS carr_desc", "`TDCVal Description` AS tdcval_desc",
     "`Load ID` AS load_id", "`GBU Description` AS gbu_desc", "`Opportunity Bucket Descritpion` AS optny_bucket_desc",
     "`Gross Weight` AS gross_weight_qty", "`Net Weight` AS net_weight_qty", "`Gross Volume` AS gross_vol_qty",
     "`Net Volume` AS net_vol_qty", "`Max Volume - TMS View` AS max_vol_trans_mgmt_sys_qty",
     "`Max Weight` AS max_weight_qty", "`Max Pallets - TMS Transport Type` AS max_pallet_tms_trans_type_qty",
     "`Distance` AS distance_qty",
     "`Gross Volume Fill Rate Weighted by Distance` AS gross_vol_fill_rate_weight_by_distance_qty",
     "`Gross Weight Fill Rate Weighted by Distance` AS gross_weight_fill_rate_weight_by_distance_qty",
     "`Net Volume Fill Rate Weighted by Distance` AS net_vol_fill_rate_weight_by_distance_qty",
     "`Net Weight Fill Rate Weighted by Distance` AS net_weight_fill_rate_weight_by_distance_qty",
     "`Origin SF` AS origin_zone_ship_from_code", "`Origin Location ID` As origin_loc_id",
     "`Destination SF` AS dest_ship_from_code", "`Destination Location ID` AS dest_loc_code",
     "`Actual Service TMS Code` AS tms_service_code",
     "`Opportunity Bucket Value` AS optny_bucket_val",
     "`True Max Weight` AS vehicle_true_max_weight_qty",
     "`True Max Volume` AS vehicle_true_max_vol_qty", "`SU per load` AS su_per_load_cnt",
     "`Total VFR Opportunity` AS total_vf_optny_amt"]

vfr_data_hub_select_exp = \
    ["acsrl_cost_amt", "actual_carr_total_trans_cost_usd_amt", "actual_cost_amt",
     "actual_cost_currency_code", "actual_goods_issue_date", "actual_trans_start_time_val",
     "carr_desc", "carr_id", "cases_impact_amt", "categ_name", "cnc_carr_mix_cost_amt",
     "country_from_code", "country_from_desc", "country_to_code", "country_to_name",
     "customer_desc", "customer_id", "customer_lvl1_desc", "customer_lvl1_code",
     "customer_lvl10_desc", "customer_lvl10_code", "customer_lvl11_desc",
     "customer_lvl11_code", "customer_lvl12_desc", "customer_lvl12_code",
     "customer_lvl2_desc", "customer_lvl2_code", "customer_lvl3_desc",
     "customer_lvl3_code", "customer_lvl4_desc", "customer_lvl4_code",
     "customer_lvl5_desc", "customer_lvl5_code", "customer_lvl6_desc",
     "customer_lvl6_code", "customer_lvl7_desc", "customer_lvl7_code",
     "customer_lvl8_desc", "customer_lvl8_code", "customer_lvl9_desc",
     "customer_lvl9_code", "default_ship_cond_code", "density_rate", "dest_loc_code",
     "dest_node_val", "dest_point_customer_id", "dest_ship_from_code", "dest_zone_code",
     "distance_multi_stop_qty", "distance_qty", "distance_shpmt_vfr_qty", "distance_uom",
     "dlvry_id", "dlvry_item_num", "material_doc_num", "dprtr_point_code", "dprtr_point_customer_id", "external_id",
     "fiscal_year_num", "fiscal_year_perd_num", "fiscal_year_variant_code", "flex_truck_order_desc",
     "floor_position_fill_rate_plan_qty", "floor_position_qty", "vfr_freight_type_code", "fuel_cost_amt",
     "gbu_code", "gbu_desc", "gi_month_num", "gross_vol_fill_rate_weight_by_distance_qty",
     "gross_vol_qty", "gross_weight_fill_rate_weight_by_distance_qty", "gross_weight_qty",
     "incrmtl_freight_auction_cost_amt", "vfr_last_update_utc_tmstp", "leg_ind_code", "linehaul_cost_amt",
     "load_gbu_id", "load_id", "low_density_site_val", "max_net_vol_qty", "max_net_weight_order_qty",
     "max_pallet_tms_trans_type_qty", "max_vol_trans_mgmt_sys_qty", "max_weight_qty", "net_density_order_qty",
     "net_vol_fill_rate", "net_vol_fill_rate_weight_by_distance_qty", "net_vol_order_qty", "net_vol_qty",
     "net_weight_fill_rate_weight_by_distance_qty", "net_weight_order_qty", "net_weight_qty",
     "oblb_floor_position_plan_qty", "oblb_gross_vol_fill_rate_plan_qty",
     "oblb_gross_vol_fill_rate_shipped_qty", "oblb_gross_vol_plan_qty", "oblb_gross_vol_shipped_qty",
     "oblb_gross_weight_fill_rate_plan_qty", "oblb_gross_weight_fill_rate_shipped_qty",
     "oblb_gross_weight_plan_qty", "oblb_gross_weight_shipped_qty", "oblb_net_vol_fill_rate_plan_qty",
     "oblb_net_vol_plan_qty", "oblb_net_weight_fill_rate_plan_qty", "oblb_net_weight_plan_qty",
     "oblb_status_code", "opertng_space_impact_amt", "opertng_space_pct", "order_num", "ordered_shipped_flag",
     "origin_loc_id", "origin_zone_code", "origin_zone_ship_from_code", "pallet_impact_amt", "pallet_impact_pct",
     "pallet_load_qty", "pallet_qty", "pallet_shipped_qty", "plant_code", "plant_code_desc", "pre_load_type_code",
     "primary_carr_flag", "prod_density_gap_impact_amt", "prod_density_gap_impact_pct", "recvng_live_drop_code",
     "recvng_site_code", "sambc_flag", "send_site_code", "ship_cond_desc", "ship_cond_val", "ship_point_code",
     "ship_point_country_to_code", "ship_point_desc", "ship_point_dest_code", "ship_point_of_dprtr_code",
     "ship_site_gbu_name", "ship_to_party_desc", "ship_to_party_id", "shipped_load_cnt", "shpmt_start_date",
     "shpmt_type_code", "sold_to_party_desc", "sold_to_party_id", "stage_dest_point_id", "stage_dprtr_point_code",
     "stage_seq_num", "table_uom", "tdcval_code", "tdcval_desc", "theortc_pallet_qty", "tms_service_code",
     "total_load_cost_amt", "total_vf_optny_amt", "trans_plan_point_code", "trans_stage_num", "true_fa_flag",
     "unsource_cost_amt", "vehicle_type_code", "vfr_not_agg_distance_qty", "vol_fill_above_100pct_flag", "vol_uom",
     "weight_avg_qty", "weight_fill_above_100pct_flag", "weight_uom", "freight_auction_flag", "tac_freight_type_code",
     "origin_freight_code", "step_factor", "trans_dest_exectn_shpmt_end_date", "trans_dest_plan_shpmt_end_date",
     "trans_dest_request_dlvry_date", "trans_origin_exectn_checkin_date", "trans_origin_plan_checkin_date",
     "trans_plan_shpmt_start_date", "follow_on_doc_num", "pallet_num_qty", "pallet_spot_qty", "total_gross_weight_qty",
     "total_gross_vol_qty", "release_date", "release_datetm", "truck_type_code", "truck_ship_to_num",
     "truck_ship_to_desc", "truck_sold_to_num", "truck_sold_to_desc", "truck_ship_point_code",
     "truck_vehicle_type_code", "vehicle_trans_medium_code", "vehicle_true_max_weight_qty", "vehicle_type2_code",
     "vehicle_axle_position_front_val", "vehicle_axle_position_back_val", "vehicle_max_axle_weight_front_qty",
     "vehicle_max_axle_weight_back_qty", "vehicle_inner_length_val", "vehicle_inner_width_val",
     "vehicle_inner_height_val", "vehicle_floorspot_footprint_num_val", "vehicle_floorspot_width_val",
     "vehicle_floorspot_length_val", "vehicle_name", "vehicle_min_back_axle_position_qty", "vehicle_true_max_vol_qty",
     "doc_flow_order_num", "doc_flow_load_id", "doc_flow_dlvry_id", "doc_flow_customer_po_num", "cut_impact_rate",
     "drf_last_truck_amt", "glb_segment_impact_cat_ld_amt", "hopade_amt", "max_orders_non_drp_amt",
     "max_orders_incrmtl_amt", "load_material_weight_qty", "load_material_vol_qty", "vehicle_fill_rate_id",
     "tfts_load_tmstp", "load_from_file_name", "channel_name", "last_update_utc_tmstp", "load_density_rate",
     "load_oblb_gross_weight_plan_qty", "load_oblb_gross_vol_plan_qty", "subsector_sales_cnt",
     "shipped_net_vol_qty", "shipped_net_weight_qty", "shipped_gross_vol_qty", "shipped_gross_weight_qty",
     "combined_load_max_weight_qty", "combined_load_max_vol_qty", "plan_gross_weight_qty", "plan_gross_vol_qty",
     "plan_net_weight_qty", "plan_net_vol_qty", "su_per_load_cnt", "load_builder_prty_val" ]

monster_vw_select_expr = \
    ["`TMS Load #` AS load_id","`Destination SAP Ship-To` AS ship_to_party_code", 
     "`Destination SAP Ship-To Desc` AS ship_to_party_desc", "`Destination SF Ship To Desc` AS dest_ship_from_code",
     "`Destination Sold To` AS dest_sold_to_name", "`Carrier ID` AS carr_num", "`Carrier Name` AS carr_desc",
     "`Origin ID` AS origin_code", "`Origin Desc` AS origin_zone_ship_from_code", 
     "`Act Trailer Check Out Date` AS actual_ship_date", "`Freight Type` AS freight_type_val",
     "`Service Code TMS` AS service_tms_code", "`Parent Carrier Name` AS parent_carr_name",
     "`CDOT On Time` AS cdot_ontime_cnt", "`# Of Shipment` AS shpmt_cnt", 
     "`# Of Shipments On Time` AS shpmt_on_time_cnt", "`# Of Measurable Shipments` AS measrbl_shpmt_cnt",
     "`Parent Location` AS parent_loc_code", "`Actual Ship Week` AS ship_week_num"]

lot_vw_select_expr = \
    ["`shipment #` AS load_id", "`actual ship date` AS actual_ship_date", "`carrier id` AS carr_num",
     "`carrier desc` AS carr_desc", "`destination location id` AS dest_ship_from_code",
     "`destination zone` AS dest_zone_val", "`freight type` AS freight_type_val", 
     "`origin location id` AS ship_point_code", "`origin zone` AS origin_zone_code", 
     "`origin zone2` AS origin_zone_ship_from_code", "`service code tms` AS actual_service_tms_code",
     "`ship week` AS ship_week_num", "`ship-to party` AS ship_to_party_code", 
     "`ship-to party description` AS ship_to_party_desc", "`otd count` AS lot_otd_cnt",
     "`tat late counter` AS lot_tat_late_counter_val", "`lot customer failure count` AS lot_cust_failure_cnt",
     "`pg failure count` AS pg_failure_cnt", "`carrier failure count` AS carr_failure_cnt",
     "`others failure count` AS others_failure_cnt", "`tolerance lot val` AS tolrnc_sot_val"]

vfr_vw_select_expr = \
    ["shpmt_start_date", "carr_id", "carr_desc", "tms_service_code", "load_id", "origin_zone_ship_from_code",
     "origin_zone_ship_from_code as origin_loc_id", "dest_ship_from_code", "dest_loc_code", "su_per_load_cnt", "plan_gross_weight_qty", "plan_net_weight_qty",
     "shipped_gross_weight_qty", "shipped_net_weight_qty", "plan_gross_vol_qty", "plan_net_vol_qty", "shipped_gross_vol_qty", 
     "shipped_net_vol_qty", "max_weight_qty", "max_vol_trans_mgmt_sys_qty", "floor_position_qty", "max_pallet_tms_trans_type_qty", 
     "cut_impact_rate", "drf_last_truck_amt", "glb_segment_impact_cat_ld_amt", "hopade_amt", "max_orders_incrmtl_amt", 
     "max_orders_non_drp_amt", "total_vf_optny_amt", "ship_point_code"]
 
tac_tender_pg_summary_vw_select_expr = \
    ["actual_carrier_total_transportation_cost_usd AS actual_carr_trans_cost_amt", "linehaul AS linehaul_cost_amt", 
     "incremental_fa AS incrmtl_freight_auction_cost_amt", "cnc_carrier_mix AS cnc_carr_mix_cost_amt", 
     "unsourced AS unsource_cost_amt", "fuel AS fuel_cost_amt", "accessorial AS acsrl_cost_amt", 
     "carrier_id", "tms_service_code", "lane", "calendar_year_week_tac", "origin_sf AS origin_zone_ship_from_code", 
     "destination_sf AS dest_ship_from_code", "carrier_description AS carr_desc", "sold_to_n AS sold_to_party_id", 
     "average_awarded_weekly_volume AS avg_award_weekly_vol_qty", "shipping_conditions AS ship_cond_val", 
     "country_from AS country_from_code", "country_to AS country_to_code", "freight_type AS freight_auction_flag", 
     "primary_carrier_flag AS primary_carr_flag", "week_begin_date AS week_begin_date", "monthtype445 AS month_type_val", 
     "calendar_year AS cal_year_num", "month_date AS month_date", "week_number AS week_num", "state_province AS region_code", 
     "accept_count AS accept_cnt", "total_count AS total_cnt", "destination_zip AS dest_postal_code", 
     "reject_count AS reject_cnt", "accept_percent AS accept_pct", "reject_percent AS reject_pct", 
     "expected_volume AS expct_vol_val", "rejects_below_award AS reject_below_award_val", "weekly_tac AS weekly_carr_rate", 
     "customer_id_description AS customer_desc", "customer AS customer_code", "customer_3_description AS customer_lvl3_desc", 
     "customer_level_5_description AS customer_lvl5_desc", "customer_level_6_description AS customer_lvl6_desc", 
     "customer_level_12_description AS customer_lvl12_desc", "customer_specific_lane AS customer_specific_lane_name"]

tac_tender_pg_summary_new_select_expr = \
    ["actual_carrier_total_transportation_cost_usd AS actual_carr_trans_cost_amt", "linehaul AS linehaul_cost_amt",
     "incremental_fa AS incrmtl_freight_auction_cost_amt", "cnc_carrier_mix AS cnc_carr_mix_cost_amt",
     "unsourced AS unsource_cost_amt", "fuel AS fuel_cost_amt", "accessorial AS acsrl_cost_amt",
     "carrier_id", "tms_service_code", "lane", "calendar_year_week_tac", "origin_sf AS origin_zone_ship_from_code",
     "destination_sf AS dest_ship_from_code", "carrier_description AS carr_desc", "sold_to_n AS sold_to_party_id",
     "average_awarded_weekly_volume AS avg_award_weekly_vol_qty", "shipping_conditions AS ship_cond_val",
     "country_from AS country_from_code", "country_to AS country_to_code", "freight_type AS freight_auction_flag",
     "primary_carrier_flag AS primary_carr_flag", "week_begin_date AS week_begin_date", "monthtype445 AS month_type_val",
     "calendar_year AS cal_year_num", "month_date AS month_date", "week_number AS week_num", "state_province AS region_code",
     "accept_count AS accept_cnt", "total_count AS total_cnt", "destination_zip AS dest_postal_code",
     "reject_count AS reject_cnt", "accept_percent AS accept_pct", "reject_percent AS reject_pct",
     "expected_volume AS expct_vol_val", "rejects_below_award AS reject_below_award_val", "weekly_tac AS weekly_carr_rate",
     "customer_id_description AS customer_desc", "customer AS customer_code", "customer_3_description AS customer_lvl3_desc",
     "customer_level_5_description AS customer_lvl5_desc", "customer_level_6_description AS customer_lvl6_desc",
     "customer_level_12_description AS customer_lvl12_desc", "customer_specific_lane AS customer_specific_lane_name",
     "`Origin Location ID` AS origin_location_id", "`campus lane name` as campus_lane_name"]
