CREATE OR REPLACE PROCEDURE urdm.sp_l_acct_loan
(
  II_DATADATE IN STRING --跑批日期，格式YYYY-MM-DD
)
/******************************
  @author:Resoft
  @create-date:2023-06-27 00:58:56
  @description:加工贷款借据信息表(L_ACCT_LOAN)表数据
  @modification history:
  m0-修改人-修改时间-修改描述
  m1-fengziyang-20230817-增加BS-5逻辑段
  m2-fengziyang-20230824-修改信贷部分账户类型码值映射
  m3-lixiang-20230905-码值表M_DICT_REMAPPING_DL贷款账户类型部分在前面补'0'
  m4-fengziyang-20230911-增加XL_6逻辑段
  m5-lixiang-20230911-修改第一段原账号取值逻辑
  m6-wky-20230914-一些贷款已经从个贷迁移到小微中，补充剔除个贷中贷款的逻辑
  m7-wky-20230918-CL_1中保证金币种未转码，补充转码逻辑
  m8-wky-20230918-XL_6修改小微贷中普惠标识，小微贷系统全部为普惠贷款
  m9-wky-20230918-CL_1调整子查询4里的中间价获取逻辑
  m10-lixiang-20231018-修改第一段（信贷）贷款资金使用位置取值逻辑
  m11-lixiang-20231101-修改第一段（信贷）原始期限取值逻辑，测试1104时吴兴文提供  
  m12-wuyuezhen-20231110-PL_2第2段个贷加工逻辑,新增-循环贷标志逻辑(信用额度信息主表取)，个贷厂商吴超提供
  m13-wuyuezhen-20231113-PL_2第2段个贷加工逻辑,修改-参考利率类型（FLOAT_TYPE）逻辑，个贷厂商路玉峰提供，BIR 基准利率，LPR LPR利率，空的都按BIR处理
  m14-lixiang-20231130-修改-CL_1第一段（信贷）计息标志字段取值逻辑，和老east保持一致
  m15-chengweixin-20231207-修改-CL_1第一段（信贷）原始期限字段取值逻辑，单位天
  m16-wuyuezhen-20231207-个贷中一些贷款在向小微贷中转移，重复贷款需要删除，改写双重嵌套子查询语法
  m17-cwx-20231226-信贷中战略新兴产业类型,工业企业技术改造升级标识修改逻辑
  m18-cwx-20240111-参照生产逻辑增加出生日期逻辑判断
  m19-lixiang-20240125-修改信贷段保证金账号，保证金比例取值逻辑（贸易融资表uat问题）
  m20-wky-20240414-I_DATADATE在该存储中获取的是10位的日期，下游使用应该是8位的，故调整
  *******************************/
IS
  V_SCHEMA    STRING; --当前存储过程所属的模式名
  V_PROCEDURE STRING; --当前储存过程名称
  V_TAB_NAME  STRING; --目标表名
  I_DATADATE  STRING; --数据日期(字符型)YYYY-MM-DD
  D_DATADATE  DATE; --数据日期(日期型)
  V_PARTID  STRING; --数据日期所在月YYYYMM
  V_STEP_ID   INT; --任务号
  V_STEP_DESC STRING; --任务描述
  V_STEP_FLAG INT; --任务执行状态标识
  V_ERRORCODE STRING; --错误编码
  V_ERRORDESC STRING; --错误内容
  V_DATA_COUNT INT; --数据变动条数
  MIN_SERIALNO STRING;  --当天最小借据号(L1信贷逻辑段用)
BEGIN
    V_STEP_ID   := 0;
    V_STEP_FLAG := 0;
    V_STEP_DESC := '参数初始化处理';
    V_SCHEMA    := 'URDM';
    V_PROCEDURE := UPPER('SP_L_ACCT_LOAN');
    V_TAB_NAME  := 'L_ACCT_LOAN';
    I_DATADATE  := II_DATADATE;
    D_DATADATE  := TO_DATE(II_DATADATE);
    V_PARTID    := TO_CHAR(D_DATADATE,'YYYYMM');
    V_DATA_COUNT := 0;

    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

    V_STEP_ID   := 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '清理 [' || 'L_ACCT_LOAN' || ']表历史数据';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

    --清理历史数据两种方式，delete或truncate
    DELETE FROM L_ACCT_LOAN WHERE DATA_DATE=TO_CHAR(D_DATADATE, 'YYYYMMDD');
    COMMIT;
    DELETE FROM URDM.PL_LBI_R_LOAN_DETAIL_HS  WHERE RPT_DT=D_DATADATE;--金数新加
     COMMIT;
    /*
    EXECUTE IMMEDIATE 'TRUNCATE TABLE L_ACCT_LOAN';
    */
	--插入数据
INSERT INTO URDM.pl_lbi_r_loan_detail_hs --金数新加
WITH ZH_INFO AS
    (SELECT L.PGS_PRCP_AMT,
            L.REPAYM_TYP,
            L.REPAYM_OPT,
            LO.LNPA_ISTU_INSTTU_CDE,
            LO.LNPA_LOAN_TYP,
            LO.LOAN_NO
       FROM omi.pl_LOAN_SCHD_TERM_hs L, omi.pl_LOAN_hs LO
      WHERE L.LNPA_ISTU_INSTTU_CDE = LO.LNPA_ISTU_INSTTU_CDE
        AND L.LNPA_LOAN_TYP = LO.LNPA_LOAN_TYP
        AND L.LOAN_NO = LO.LOAN_NO
        AND L.TERM_NO =
            (SELECT MAX(TERM_NO)
               FROM omi.pl_LOAN_SCHD_TERM_hs
              WHERE LNPA_ISTU_INSTTU_CDE = LO.LNPA_ISTU_INSTTU_CDE
                AND LNPA_LOAN_TYP = LO.LNPA_LOAN_TYP
                AND LOAN_NO = LO.LOAN_NO
                AND begndt<=D_DATADATE AND overdt>D_DATADATE AND partid=V_PARTID)
        AND LO.DEBT_STS <> "CHRGO"
        AND L.begndt<=D_DATADATE AND L.overdt>D_DATADATE AND L.partid=V_PARTID
        AND LO.begndt<=D_DATADATE AND LO.overdt>D_DATADATE AND LO.partid=V_PARTID
     ), DEBX_INFO AS
    (SELECT COUNT(1) IP, L.LNPA_ISTU_INSTTU_CDE, L.LNPA_LOAN_TYP, L.LOAN_NO
       FROM omi.pl_LOAN_SCHD_TERM_hs LS, omi.pl_LOAN_hs L
      WHERE LS.LNPA_ISTU_INSTTU_CDE = L.LNPA_ISTU_INSTTU_CDE
        AND LS.LNPA_LOAN_TYP = L.LNPA_LOAN_TYP
        AND LS.LOAN_NO = L.LOAN_NO
        AND LS.REPAYM_OPT = "IO"
        AND L.DEBT_STS <> "CHRGO"
        AND LS.begndt<=D_DATADATE AND LS.overdt>D_DATADATE AND LS.partid=V_PARTID
        AND L.begndt<=D_DATADATE AND L.overdt>D_DATADATE AND L.partid=V_PARTID
      GROUP BY L.LNPA_ISTU_INSTTU_CDE, L.LNPA_LOAN_TYP, L.LOAN_NO),
LOAN_LOAN_INFO AS( --还款方式
     SELECT L.LOAN_NO,
             L.LNPA_ISTU_INSTTU_CDE,
             L.LNPA_LOAN_TYP,
             CASE
               WHEN NVL(LP.IRREG_IND, "N") = "Y" THEN "随心还还款法"
               WHEN NVL(LP.INSTM_LN_IND, "N") = "Y" THEN
                CASE
                  WHEN NVL(L.TERM_COMBINE_IND, "N") = "Y" THEN "组合还款"
                  ELSE
                   CASE
                     WHEN NVL(L.BALLOON_IND, "N") = "Y" THEN "气球贷"
                     WHEN NVL(T1.PGS_PRCP_AMT, 0) > 0 THEN "等额递增还款"
                     WHEN NVL(T1.PGS_PRCP_AMT, 0) < 0 THEN "等额递减还款"
                     WHEN NVL(T2.IP, 0) = 1 THEN "净息还款"
                     ELSE
                      CASE
                        WHEN NVL(T2.IP, 0) = 0 THEN
                         CASE
                           WHEN T1.REPAYM_TYP = "T" THEN "等额本息"
                           WHEN T1.REPAYM_TYP = "A" THEN "固定期供金额"
                           ELSE
                            CASE
                              WHEN T1.REPAYM_TYP = "P" THEN
                               CASE
                                 WHEN NVL(PH.PRCP_PAYM_FREQ, 0) <> NVL(PH.INT_PAYM_FREQ, 1) THEN "本息间隔不一致"
                                 ELSE "等额本金"
                               END
                            END
                         END
                      END
                   END
                END

               ELSE
                CASE
                  WHEN NVL(T2.IP, 0) > 0 THEN "按期还息到期还本"
                  ELSE "利随本清"
                END
             END REPAYMENT_TYP
       FROM omi.pl_LOAN_hs L, omi.pl_LOAN_PARA_hs LP, ZH_INFO T1, DEBX_INFO T2, omi.pl_PMSH_HDR_hs PH
      WHERE L.LNPA_LOAN_TYP = LP.LOAN_TYP
        AND L.LNPA_ISTU_INSTTU_CDE = T1.LNPA_ISTU_INSTTU_CDE
        AND L.LNPA_LOAN_TYP = T1.LNPA_LOAN_TYP
        AND L.LOAN_NO = T1.LOAN_NO
        AND L.LNPA_ISTU_INSTTU_CDE = T2.LNPA_ISTU_INSTTU_CDE(+)
        AND L.LNPA_LOAN_TYP = T2.LNPA_LOAN_TYP(+)
        AND L.LOAN_NO = T2.LOAN_NO(+)
        AND L.LNPA_ISTU_INSTTU_CDE = PH.LOAN_LNPA_ISTU_INSTTU_CDE
        AND L.LNPA_LOAN_TYP = PH.LOAN_LNPA_LOAN_TYP
        AND L.LOAN_NO = PH.LOAN_LOAN_NO
        AND L.STS NOT IN ("NBAP", "REVS")
        AND L.DEBT_STS <> "CHRGO"
        AND PH.begndt<=D_DATADATE AND PH.overdt>D_DATADATE AND PH.partid=V_PARTID
        AND LP.begndt<=D_DATADATE AND LP.overdt>D_DATADATE AND LP.partid=V_PARTID
        AND L.begndt<=D_DATADATE AND L.overdt>D_DATADATE AND L.partid=V_PARTID
     ),
LOAN_MONTH_NEW AS
    (SELECT LB.INSTTU_CDE, LB.LOAN_TYP,LB.LOAN_NO,LB.LOAN_GRD, LB.OS_DAYS, LB.OS_PRCP, LB.RECV_INT, LB.SUSP_INT
       FROM omi.pl_LOAN_MONTH_BAL_hs LB
      WHERE tdh_todate(BAL_DATE) = D_DATADATE
      AND LB.begndt<=D_DATADATE AND LB.overdt>D_DATADATE AND LB.partid=V_PARTID
     UNION
     SELECT L.LNPA_ISTU_INSTTU_CDE INSTTU_CDE,L.LNPA_LOAN_TYP LOAN_TYP,TO_CHAR(L.LOAN_NO),L.LOAN_GRD,0 OS_DAYS, 0 OS_PRCP, 0 RECV_INT, 0 SUSP_INT
       FROM omi.pl_LOAN_hs L
      WHERE tdh_todate(L.ACTV_DT) >= TRUNC(D_DATADATE, "YYYY")
        AND tdh_todate(L.ACTV_DT) <= D_DATADATE
        AND L.STS = "SETL"
        AND L.begndt<=D_DATADATE AND L.overdt>D_DATADATE AND L.partid=V_PARTID),
SUM_ORIG_PRCP AS--授信额度(人行)
     (SELECT L.CTIF_ID_TYPE,L.CTIF_ID_NO,L.CTIF_ISS_CTRY,
      SUM(L.ORIG_PRCP) SUM_ORIG_PRCP,
      SUM(CASE WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("14", "16") OR
                (SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("12","18") AND
                              (SELECT IS_OPERATE FROM omi.pl_PRPS_FIN_hs PF WHERE PF.PRPS_CDE=NVL(L.PRPS_FIN,"18") AND PF.begndt<=D_DATADATE AND PF.overdt>D_DATADATE AND PF.partid=V_PARTID )="Y" ) THEN L.ORIG_PRCP ELSE 0 END  ) SUM_ORIG_OPR,
       SUM(CASE WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("10", "11", "13") OR
                (SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("12","18") AND
                              (SELECT NVL(IS_OPERATE,"N") FROM omi.pl_PRPS_FIN_hs PF WHERE PF.PRPS_CDE=NVL(L.PRPS_FIN,"18") AND PF.begndt<=D_DATADATE AND PF.overdt>D_DATADATE AND PF.partid=V_PARTID )<>"Y" ) THEN L.ORIG_PRCP ELSE 0 END  ) SUM_ORIG_CON
      FROM omi.pl_LOAN_hs L ,omi.pl_LOAN_PARA_hs LP
      WHERE L.STS="ACTV"
      AND L.ORIG_PRCP>0
      --经普惠张通确认，额度判断排除余额等于0的贷款
      AND L.OS_PRCP>0
      AND L.LNPA_LOAN_TYP=LP.LOAN_TYP
      AND NVL(LP.PF_IND, "N") = "N" --排除公积金贷款
      AND NVL(LP.TRUST_LN_IND, "N") = "N"
      AND LP.begndt<=D_DATADATE AND LP.overdt>D_DATADATE AND LP.partid=V_PARTID
      AND L.begndt<=D_DATADATE AND L.overdt>D_DATADATE AND L.partid=V_PARTID
      GROUP BY L.CTIF_ID_TYPE,L.CTIF_ID_NO,L.CTIF_ISS_CTRY
      )
SELECT D_DATADATE rpt_dt,
           L.LOAN_NO,
           L.LNPA_LOAN_TYP loan_typ,
           (CASE
             WHEN C.ADDR1_TYPE = "H" THEN
              (select P.PROVINCE_DESC from omi.pl_PROVINCE_CDE_hs P WHERE P.PROVINCE_CDE = C.ADDR1_PROVINCE_CDE AND P.begndt<=D_DATADATE AND P.overdt>D_DATADATE AND P.partid=V_PARTID)||(select CI.CITY_DESC from omi.pl_CITY_hs CI WHERE CI.CITY_CDE = C.ADDR1_LCT_CDE AND CI.begndt<=D_DATADATE AND CI.overdt>D_DATADATE AND CI.partid=V_PARTID)||(select D.DIST_NAM from omi.pl_DISTRICT_hs D WHERE D.DIST_CDE = C.ADDR1_DIST_CDE AND D.begndt<=D_DATADATE AND D.overdt>D_DATADATE AND D.partid=V_PARTID) ||C.ADDR1_L1 || C.ADDR1_L2 || C.ADDR1_L3 || C.ADDR1_L4
             WHEN C.ADDR2_TYPE = "H" THEN
              (select P.PROVINCE_DESC from omi.pl_PROVINCE_CDE_hs P WHERE P.PROVINCE_CDE = C.ADDR2_PROVINCE_CDE AND P.begndt<=D_DATADATE AND P.overdt>D_DATADATE AND P.partid=V_PARTID)||(select CI.CITY_DESC from omi.pl_CITY_hs CI WHERE CI.CITY_CDE = C.ADDR2_LCT_CDE AND CI.begndt<=D_DATADATE AND CI.overdt>D_DATADATE AND CI.partid=V_PARTID)||(select D.DIST_NAM from omi.pl_DISTRICT_hs D WHERE D.DIST_CDE = C.ADDR2_DIST_CDE AND D.begndt<=D_DATADATE AND D.overdt>D_DATADATE AND D.partid=V_PARTID)||C.ADDR2_L1 || C.ADDR2_L2 || C.ADDR2_L3 || C.ADDR2_L4
             WHEN C.ADDR3_TYPE = "H" THEN
              (select P.PROVINCE_DESC from omi.pl_PROVINCE_CDE_hs P WHERE P.PROVINCE_CDE = C.ADDR3_PROVINCE_CDE AND P.begndt<=D_DATADATE AND P.overdt>D_DATADATE AND P.partid=V_PARTID)||(select CI.CITY_DESC from omi.pl_CITY_hs CI WHERE CI.CITY_CDE = C.ADDR3_LCT_CDE AND CI.begndt<=D_DATADATE AND CI.overdt>D_DATADATE AND CI.partid=V_PARTID)||(select D.DIST_NAM from omi.pl_DISTRICT_hs D WHERE D.DIST_CDE = C.ADDR3_DIST_CDE AND D.begndt<=D_DATADATE AND D.overdt>D_DATADATE AND D.partid=V_PARTID)||C.ADDR3_L1 || C.ADDR3_L2 || C.ADDR3_L3 || C.ADDR3_L4
             WHEN C.ADDR4_TYPE = "H" THEN
              C.ADDR4_L1 || C.ADDR4_L2 || C.ADDR4_L3 || C.ADDR4_L4
             WHEN C.ADDR5_TYPE = "H" THEN
              C.ADDR5_L1 || C.ADDR5_L2 || C.ADDR5_L3 || C.ADDR5_L4
           END) HOME_ADDR,
           L.LOAN_TNR,
           L.PRPS_FIN,
           (CASE
             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("14", "16") THEN
              "经营"
             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("12", "18") THEN
              CASE
                WHEN (SELECT IS_OPERATE
                        FROM omi.pl_PRPS_FIN_hs PF
                       WHERE PF.PRPS_CDE = NVL(L.PRPS_FIN, "18") AND PF.begndt<=D_DATADATE AND PF.overdt>D_DATADATE AND PF.partid=V_PARTID) = "Y" THEN
                 "经营"
                ELSE
                 "消费"
              END

             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("10", "11", "13") THEN
              "消费"
           END) loan_grp,--YWLX,
           (CASE
             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("14", "16") THEN
              "经营"
             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("12", "18") THEN
              CASE
                WHEN (SELECT IS_OPERATE
                        FROM omi.pl_PRPS_FIN_hs PF
                       WHERE PF.PRPS_CDE = NVL(L.PRPS_FIN, "18")AND PF.begndt<=D_DATADATE AND PF.overdt>D_DATADATE AND PF.partid=V_PARTID ) = "Y" THEN
                 "经营"
                ELSE
                 "消费"
              END
              WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) = 10 THEN
                /*CASE
                 WHEN
                 (select case when concat_ws(",",collect_set(p.clas_class_cde)) LIKE "%SP%" then "Y" else "N" end
                    from omi.pl_colt_idx_hs ci, omi.pl_ppty_hs p
                   where ci.clcm_colt_itm = p.clcm_colt_itm
                     and ci.loan_loan_no = l.loan_no 
                     AND ci.begndt<=D_DATADATE AND ci.overdt>D_DATADATE AND ci.partid=V_PARTID
                     AND p.begndt<=D_DATADATE AND p.overdt>D_DATADATE AND p.partid=V_PARTID
                     ) = "Y" and SUBSTR(L.Bhdt_Bch_Cde, 0, 3) = "010" THEN
                  "经营"
                 ELSE
                  "消费"
                END        
             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("11", "13") THEN
              "消费"
           END*/
CASE
                 WHEN
                 (
				 	select case when p.LSREALTYTYPE = "SP" and A2.CLRTYPEID="03010204" then "Y" else "N" end
					from omi.pl_colt_idx_hs ci
					INNER JOIN omi.pl_colt_com_hs com
					ON ci.clcm_colt_itm =com.colt_itm
					and com.clty_colt_typ in ("PTY","LND")
					AND com.partid = V_PARTID AND com.begndt <= D_DATADATE AND com.overdt > D_DATADATE
					inner join  omi.col_clr_info_hs A2  
					on com.colt_itm = A2.clrid 
					and A2.sourcesystem = "02"
					and A2.begndt <= D_DATADATE 
					and A2.overdt > D_DATADATE 
					and A2.partid = V_PARTID 
					inner JOIN OMI.COL_CLR_ASSET_REALTY_HS P
					ON A2.CLRID=P.CLRID
					AND P.BEGNDT<=D_DATADATE
					AND P.OVERDT>D_DATADATE
					AND P.PARTID=V_PARTID
					where ci.begndt <= D_DATADATE 
					and ci.overdt > D_DATADATE 
					and ci.partid = V_PARTID
					and ci.loan_loan_no = l.loan_no 
				) = "Y" and SUBSTR(L.Bhdt_Bch_Cde, 0, 3) = "010" THEN
                  "经营"
                 ELSE
                  "消费"
				  END
             WHEN SUBSTR(L.LNPA_LOAN_TYP, 0, 2) IN ("11", "13") THEN
              "消费"
           END   --按老金数lsphjc修改
) loan_grp2,--YWLX2,
           C.BR_NO ADDR_TYP,
           L.LOAN_TARGET,
           LI.REPAYMENT_TYP PAY_TYP,
           II.FIN_INSTTU BUSS_TYP,
           CASE
             WHEN II.FIN_INSTTU = "6" AND NVL(II.FIN_INSTTU_2, "N") = "Y" THEN
              "个体工商户"
             WHEN II.FIN_INSTTU IN ("4", "5") AND
                  NVL(II.FIN_INSTTU_2, "N") = "Y" THEN
              "小微企业主"
             ELSE
              "否"
           END MINBUSS,--GSH,
           C.IDSY_PFSN,
           II.EMPR,
           II.EMP_CONTACT_ADD,
           NVL(II.PLACE_ORIGN, "LOCAL") PLACE_ORIGN,
           L.AOFR_ACCT_OFFCR_CDE ACCT_OFFCR,
           LB.LOAN_GRD,
           NVL(LB.OS_DAYS,0) OS_DAYS,
           L.BHDT_BCH_CDE BCH_CDE,
           C.ID_NO,
           C.ID_TYPE,
           C.ISS_CTRY,
           C.CUST_NAM,
           L.ACTV_DT,
           L.LAST_DUE_DT,
           L.ORIG_PRCP,
           LB.OS_PRCP,
           L.BASE_RATE,
           L.INT_ADJ_PCT,
           L.SPRD,
           L.MAIN_GUR_TYP,
           LB.RECV_INT,
           LB.SUSP_INT,
           NVL(R.OD_DAY_COUNT,0) OD_PRCP_DAY,
           NVL(R.OD_PRCP,0) OD_PRCP_AMT,
           NVL(R.OD_INT_DAY_COUNT,0) OD_INT_DAY,
           NVL(R.OD_BN_INT,0)+NVL(R.OD_BW_INT,0)-NVL(R.OD_INT,0) OD_INT_AMT,
           NVL(R.OD_OD_INT_DAY_COUNT,0) OD_OD_INT_DAY,
           NVL(R.OD_INT,0) OD_OD_INT_AMT,
           L.STS,
           L.PROD_CDE FIRST_HOS,
		   " " INC_HOS,
           NVL(S.SUM_ORIG_PRCP,0) SUM_ORIG_PRCP,
           NVL(S.SUM_ORIG_OPR,0) SUM_ORIG_OPR,
           NVL(S.SUM_ORIG_CON,0) SUM_ORIG_CON,
          L.RECOURSE IS_LOAN_GREEN, --是否为绿色贷款
          L.CUST_REF LOAN_TARGET_GREEN, --绿色贷款投向
    (CASE
          WHEN L.REFINC_FURCHRG_IND = "E" THEN
          "续贷"
          WHEN L.REFINC_FURCHRG_IND = "R" THEN
          "借新还旧"
          WHEN L.REFINC_FURCHRG_IND IS NULL THEN
          "正常"
          ELSE
          "其它"
          END
          ) LOAN_IND
      FROM omi.pl_LOAN_hs           L,
           LOAN_MONTH_NEW LB,
           omi.pl_INDIV_INFO_hs     II,
           omi.pl_CUST_INFO_hs      C,
           LOAN_LOAN_INFO LI,
           SUM_ORIG_PRCP  S,
           (SELECT * FROM omi.pl_RPT_LN_OS_DTL_hs R WHERE tdh_todate(R.RPT_DT) = D_DATADATE AND R.begndt<=D_DATADATE AND R.overdt>D_DATADATE AND R.partid=V_PARTID) R
     WHERE L.CTIF_ID_TYPE = II.CTIF_ID_TYPE
       AND L.CTIF_ID_NO = II.CTIF_ID_NO
       AND L.CTIF_ISS_CTRY = II.CTIF_ISS_CTRY
       AND L.CTIF_ID_TYPE = C.ID_TYPE
       AND L.CTIF_ID_NO = C.ID_NO
       AND L.CTIF_ISS_CTRY = C.ISS_CTRY
       AND L.CTIF_ID_TYPE = S.CTIF_ID_TYPE(+)
       AND L.CTIF_ID_NO = S.CTIF_ID_NO(+)
       AND L.CTIF_ISS_CTRY = S.CTIF_ISS_CTRY(+)
       AND L.LNPA_ISTU_INSTTU_CDE = LB.INSTTU_CDE
       AND L.LNPA_LOAN_TYP = LB.LOAN_TYP
       AND L.LOAN_NO = LB.LOAN_NO
       AND L.LNPA_ISTU_INSTTU_CDE = LI.LNPA_ISTU_INSTTU_CDE(+)
       AND L.LNPA_LOAN_TYP = LI.LNPA_LOAN_TYP(+)
       AND L.LOAN_NO = LI.LOAN_NO(+)
       AND L.LNPA_LOAN_TYP = R.LNPA_LOAN_TYP(+)
       AND L.LOAN_NO = R.LOAN_NO(+)
       AND L.DEBT_STS <> "CHRGO"
       AND C.begndt<=D_DATADATE AND C.overdt>D_DATADATE AND C.partid=V_PARTID
       AND II.begndt<=D_DATADATE AND II.overdt>D_DATADATE AND II.partid=V_PARTID
       AND L.begndt<=D_DATADATE AND L.overdt>D_DATADATE AND L.partid=V_PARTID;
    V_DATA_COUNT := SQL%ROWCOUNT;
    V_STEP_FLAG := 1;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

    V_STEP_ID   := V_STEP_ID + 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '第1段加工逻辑';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);
    -- 20230630.kongyongjie.add-获取当期最小借据号
    SELECT MIN(SERIALNO) INTO MIN_SERIALNO FROM OMI.CL_BUSINESS_DUEBILL_HS WHERE BEGNDT <= D_DATADATE AND OVERDT > D_DATADATE AND PARTID = V_PARTID;

    INSERT INTO urdm.L_ACCT_LOAN 
    (ACCT_NUM,  --合同号
     ACCT_STS,  --账户状态
	 ACCT_STS_FHZ,
	 ACCT_STS_XMDK,
     ACCT_TYP,  --账户类型
     ACCT_TYP_DESC,  --账户类型说明
     ACCU_INT_AMT,  --应计利息
     ACCU_INT_FLG,  --计息标志
     ACTUAL_MATURITY_DT,  --实际到期日期
     BASE_INT_RAT,  --基准利率
     BOOK_TYPE,  --账户种类
     CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
     CANCEL_FLG,  --核销标志
     CIRCLE_LOAN_FLG,  --循环贷款标志
     CONSECUTE_TERM_NUM,  --连续欠款期数
     CUMULATE_TERM_NUM,  --累计欠款期数
     CURRENT_TERM_NUM,  --当前还款期数
     CURR_CD,  --币种
     CUST_ID,  --客户号
     DATA_DATE,  --数据日期
     DATE_SOURCESD,  --数据来源
     DEPARTMENTD,  --归属部门
     DRAWDOWN_AMT,  --放款金额
     DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
     DRAWDOWN_DT,  --放款日期
     DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
     DRAWDOWN_TYPE,  --放款方式
     DRAWDOWN_TYPE_NEW,  --发放类型
     EMP_ID,  --信贷员员工号
     ENTRUST_PAY_AMT,  --受托支付金额
     EXTENDTERM_FLG,  --展期标志
     FINISH_DT,  --结清日期
     FLOAT_TYPE,  --参考利率类型
     FUND_USE_LOC_CD,  --贷款资金使用位置
     GENERALIZE_LOAN_FLG,  --普惠型贷款标志  迁移至小微系统，此处默认空
     SP_PROV_AMT,  --专向准备
     GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
     --GREEN_BALANCE,  --绿色信贷余额   薄天开反馈S68手工,此处留空
     GREEN_LOAN_FLG,  --绿色贷款标志
     GREEN_LOAN_TYPE,  --绿色贷款用途分类
     GUARANTY_TYP,  --贷款担保方式
     INDEPENDENCE_PAY_AMT,  --自主支付金额
     INDUST_STG_TYPE,  --战略新兴产业类型
     INDUST_TRAN_FLG,  --工业企业技术改造升级标识
     INTERNET_LOAN_FLG,  --互联网贷款标志
     INT_RATE_TYP,  --利率类型
     INT_REPAY_FREQ,  --利息还款频率
     INT_REPAY_FREQ_DESC,  --利息还款频率说明
     IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
     IS_REPAY_OPTIONS,  --是否内嵌提前还款权
     ITEM_CD,  --科目号
     I_OD_DT,  --利息逾期日期
     LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
     LOAN_ACCT_BAL,  --贷款余额
     LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
     LOAN_ACCT_NAME,  --贷款入账账户名称
     LOAN_ACCT_NUM,  --贷款入账账号
     LOAN_BUSINESS_TYP,  --贷款用途分类
     LOAN_BUY_INT,  --是否转入贷款
     LOAN_FHZ_NUM,  --贷款分户账账号
     LOAN_GRADE_CD,  --五级分类代码
     LOAN_KIND_CD,  --贷款形式
     LOAN_NUM,  --贷款编号
     LOAN_NUM_OLD,  --原贷款编号
     LOAN_PURPOSE_AREA,  --贷款投向地区
     LOAN_PURPOSE_CD,  --贷款投向
	 LOAN_PURPOSE_COUNTRY_NEW,--贷款投向国别
     LOAN_RATIO,  --出资比例
     --LOAN_RESCHED_DT,  --贷款重组日期  薄天开反馈人工，补录
     LOAN_SELL_INT,  --转出标志
     LOW_RISK_FLAG,  --是否低风险业务
     MAIN_GUARANTY_TYP,  --主要担保方式
     MATURITY_DT,  --原始到期日期
     YEAR_INT_INCOM,  --本年利息收入
     NEXT_INT_PAY_DT,  --下一付息日
     NEXT_REPRICING_DT,  --下一利率重定价日
     OD_DAYS,  --逾期天数
     OD_FLG,  --逾期标志
     OD_INT,  --逾期利息
     OD_INT_OBS,  --表外欠息
     OD_LOAN_ACCT_BAL,  --逾期贷款余额
     ORG_NUM,  --机构号
     ORIG_ACCT_NO,  --原账号
     ORIG_TERM,  --原始期限
     ORIG_TERM_TYPE,  --原始期限类型
     OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
     PAY_ACCT_BANK,  --还款账号所属行名称
     PAY_ACCT_NUM,  --还款账号
     PFTZ_LOAN_FLG,  --自贸区贷款标志
     POVERTY_ALLE,  --已脱贫人口贷款标志
     PRICING_BASE_TYPE,  --定价基础类型
     P_OD_DT,  --本金逾期日期
     REAL_INT_RAT,  --实际利率
     RENEW_FLG,  --无还本续贷标志
     REPAY_FLG,  --借新还旧标志
     REPAY_TERM_NUM,  --还款总期数
     REPAY_TYP,  --还款方式_2
     REPAY_TYP_DESC,  --还款方式说明
     --RESCHED_FLG,  --重组标志   薄天开反馈人工，补录
     SECURITY_ACCT_NUM,  --保证金账号
     SECURITY_AMT,  --保证金金额
     SECURITY_CURR,  --保证金币种
     SECURITY_RATE,  --保证金比例
     TAX_RELATED_FLG,  --银税合作贷款标志
     TRANSFERS_LOAN_FLG,  --转贷款标志
     UNDERTAK_GUAR_TYPE,  --创业担保贷款类型
     USEOFUNDS,  --贷款用途
     YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志
	 LOAN_PURPOSE_COUNTRY,  --贷款投向国别
     PPL_REPAY_FREQ, --本金还款频率
     INT_ADJEST_AMT,  --利息调整
     REMARK  --备注
	 ,ORIG_TERM_BC	--贷款原始期限-合同
	 ,DRAWDOWN_NUM --放款编号
	 ,guaranty_typ_js
     ,CUST_ID_JS  --客户号_金数
	 ,PRODUCT_TYPE_JS
	 ,LCSTATUS --信用证状态 EAST用 20241113
	 ,ACCT_NUM_CQCS  --业务合同号客户风险
    )
    WITH SUBQUERY_1 AS 
           (SELECT DISTINCT ITEMNO,ITEMNAME,BANKNO,BEGNDT,OVERDT,PARTID 
            FROM OMI.CL_CODE_LIBRARY_HS  
			WHERE CODENO IN ('Currency')
			  AND BEGNDT <= D_DATADATE 
			  AND OVERDT > D_DATADATE 
			  AND PARTID = V_PARTID),
         /*20230627.kongyongjie.此子查询未用到且逻辑待确认，先删除
		 SUBQUERY_2 AS
           (SELECT KDK.DKJIEJUH,KDK.DZHKRIQI,KDK.HUANBJEE
            FROM OMI.KN_KLNB_DKHBJH_HS KDK,
            (SELECT C.DKJIEJUH,MIN(C.DZHKRIQI) AS XYCIHKRQ 
             FROM OMI.KN_KLNB_DKHBJH_HS C
             WHERE C.CHULIZHT='0'
               AND C.DZHKRIQI>(SELECT D.JIAOYIRQ FROM OMI.KN_KAPP_SYSDAT_HS D) 
			 GROUP BY C.DKJIEJUH) E
            WHERE KDK.DKJIEJUH=E.DKJIEJUH
            AND KDK.DZHKRIQI=E.XYCIHKRQ
            AND KDK.CHULIZHT='0'),*/
         SUBQUERY_3 AS 
           (SELECT A.DKJIEJUH, COUNT(*) AS LEIJQXQS 
            FROM OMI.KN_KLNB_DKZHQG_HS A
            WHERE (A.YSQIANXI+A.CSQIANXI+A.YSHOFAXI+A.CSHOFAXI)>0
              AND A.BENQIZHT='1'
			  AND A.BEGNDT <= D_DATADATE 
              AND A.OVERDT > D_DATADATE 
              AND A.PARTID = V_PARTID
            GROUP BY A.DKJIEJUH),
         SUBQUERY_4 AS 
           (SELECT BD2.CUSTOMERID, SUM(NVL(BD2.BUSINESSSUM,0)) AS BALANCESUM 
            FROM OMI.CL_BUSINESS_DUEBILL_HS BD2
	        INNER JOIN OMI.CL_ENT_INFO_HS EI2
	                ON BD2.CUSTOMERID=EI2.CUSTOMERID
	               AND EI2.BEGNDT <= D_DATADATE 
	               AND EI2.OVERDT > D_DATADATE 
	               AND EI2.PARTID = V_PARTID
            WHERE BD2.CUSTOMERID=EI2.CUSTOMERID
              AND ((BD2.BUSINESSTYPE LIKE '1%' AND BD2.BUSINESSTYPE<>'1020050')OR (BD2.BUSINESSTYPE LIKE '2%' AND BD2.BUSINESSTYPE <>'2070'))
              AND BD2.BALANCE>0
              AND NVL(EI2.SCOPE,'5') IN ('4','5')
              AND BD2.BEGNDT <= D_DATADATE 
	          AND BD2.OVERDT > D_DATADATE 
	          AND BD2.PARTID = V_PARTID
            GROUP BY BD2.CUSTOMERID),
         SUBQUERY_5 AS 
           (SELECT KX.DKJIEJUH,SUM(KX.YSYJLIXI+KX.YSYJFAXI-KX.YSYJXXSH) AS S1 
			FROM OMI.KN_KLNL_DKJXMX_HS KX 
			WHERE KX.JIXIRIQI BETWEEN TRUNC(D_DATADATE,'YYYY') AND LAST_DAY(D_DATADATE)
			GROUP BY KX.DKJIEJUH),
		  NBFHZ AS 
            (SELECT ZHANGHAO FROM OMI.KN_KFAA_NBFHZH_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT ZHANGHAO FROM OMI.KN_KTAA_NBFHZH_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT ZHANGHAO FROM OMI.KN_KTAB_CFTYDQ_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT ZHANGHAO FROM OMI.KN_KTAB_CFTYHQ_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT DAIXZHXH AS ZHANGHAO  FROM OMI.KN_KTAB_DXZDJB_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID ),
		  NBFHZ_1 AS 
            (SELECT ZHANGHAO FROM OMI.KN_KFAA_NBFHZH_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT ZHANGHAO FROM OMI.KN_KTAA_NBFHZH_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT ZHANGHAO FROM OMI.KN_KTAB_CFTYDQ_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT ZHANGHAO FROM OMI.KN_KTAB_CFTYHQ_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID UNION 
             SELECT DAIXZHXH AS ZHANGHAO  FROM OMI.KN_KTAB_DXZDJB_HS WHERE BEGNDT<=D_DATADATE AND OVERDT>D_DATADATE AND PARTID=V_PARTID )	 
   SELECT NVL(BC.ARTIFICIALNO,BD.RELATIVESERIALNO2) AS ACCT_NUM,  --合同号
          CASE 	  
		   WHEN bd.FINISHDATE IS NULL and  kjj.kemumnch='企业委托贷款' AND bc.classifyresult IN ('01') THEN '1' --add by SPE-20240715-0065-申请修改EAST报送“企业委托贷款”的贷款状态取数逻辑和“委托人名称”取数逻辑-吴秋鸿
			   WHEN bd.FINISHDATE IS NULL and  kjj.kemumnch='企业委托贷款' AND bc.classifyresult IN ('02','03','04','05') THEN '2' --add by SPE-20240715-0065-申请修改EAST报送“企业委托贷款”的贷款状态取数逻辑和“委托人名称”取数逻辑-吴秋鸿
		  /*WHEN BD.OVERDUEBALANCE+BD.DULLBALANCE+BD.BADBALANCE+BD.INTERESTBALANCE1+BD.INTERESTBALANCE2+BD.FINEBALANCE1+BD.FINEBALANCE2>0 AND bd.BUSINESSTYPE <>'2070' THEN '2'*/
		  WHEN nvl( round(
                             (nvl(bd.OVERDUEBALANCE, 0) + nvl(bd.DULLBALANCE, 0) +	--SPE-20240813-0067 修改欠本金额和欠本日期逻辑
                             nvl(bd.BADBALANCE, 0)),
                             2),
                    0)+BD.INTERESTBALANCE1+BD.INTERESTBALANCE2+BD.FINEBALANCE1+BD.FINEBALANCE2>0 AND bd.BUSINESSTYPE <>'2070' THEN '2'
            WHEN BD.FINISHDATE IS NOT NULL THEN '3'
             -- WHEN KD.DKZHHZHT ='0' THEN '1' 
               -- WHEN KD.DKZHHZHT='1' THEN '3' 
			   ELSE '1'
          END AS ACCT_STS,  --账户状态
		CASE  WHEN KD.DKZHHZHT ='0' THEN '1' 
               WHEN KD.DKZHHZHT='1' THEN '3' 
			   ELSE '1'
		END AS ACCT_STS_FHZ, --GXH 账户状态 DGXDFHZ 20240611
		CASE  WHEN bd.OVERDUEBALANCE+bd.DULLBALANCE+bd.BADBALANCE>0 THEN '2' 
               when bd.FINISHDATE is not null  THEN '3' 
	    ELSE '1'
		END AS ACCT_STS_XMDK, --GXH 账户状态 XMDKXXB 20240611
          --NVL(M1.NEW_VALUES,'099901') AS ACCT_TYP,  --账户类型
		  CASE WHEN BD.serialno = '11850000cdjj002' THEN '099903' ELSE 
          NVL(M1.NEW_VALUES,'099901') END AS ACCT_TYP,  --账户类型
          CASE WHEN BT.TYPENAME='银团贷款' THEN '银团贷款'
		  WHEN BT.TYPENAME='对公汽车按揭贷款' THEN '对公汽车按揭贷款'
 		  END AS ACCT_TYP_DESC,  --账户类型说明
          KD.YSYJLIXI+KD.YSYJFAXI AS ACCU_INT_AMT,  --应计利息
          CASE WHEN BC.ICType='010' THEN 'N' ELSE 'Y' END AS ACCU_INT_FLG,  --计息标志
          TO_DATE(BD.ACTUALMATURITY) AS ACTUAL_MATURITY_DT,  --实际到期日期
         -- BC.BASERATE AS BASE_INT_RAT,  --基准利率
         --原金数对基准利率处理 需要跟李曜确认修改是否合理 wzm 
          CASE 
			WHEN BC.ICTYPE in("020","150") THEN "" --ICTYPE计息方式
			ELSE to_char(round(BC.BaseRate,5),"999999999999999999.00000") END AS BASE_INT_RAT,  --基准利率
          '2' AS BOOK_TYPE,  --账户种类
          'N' AS CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
          CASE WHEN KD.DKZHHZHT='2' THEN 'Y' ELSE 'N'
          END AS CANCEL_FLG,  --核销标志
          --CASE WHEN BC.BUSINESSTYPE LIKE '1010030' THEN 'Y' ELSE 'N'END AS CIRCLE_LOAN_FLG,  --循环贷款标志
          'N' AS CIRCLE_LOAN_FLG,  --循环贷款标志  --by gjw 20240326 目前生产报送1104法透没有算循环贷
          -- CASE WHEN NVL(BD.INTERESTBALANCE1,0)+NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE1,0)+NVL(BD.FINEBALANCE2,0)>0 THEN NVL(L.CONSECUTE_TERM_NUM,0) + 1
		       -- ELSE 0 
		  -- END AS CONSECUTE_TERM_NUM,  --连续欠款期数
		  0 AS CONSECUTE_TERM_NUM,  --连续欠款期数 BY QZJ 20240204 目前生产east对公信贷业务借据表信贷部分连续欠款期数均为0，后续确认是否确定该修改
		  /*CASE
          WHEN (
           nvl(case
               WHEN bd.BUSINESSTYPE not IN ('1040050', '1040010') and
                    TO_DATE(from_unixtime(unix_timestamp(bd.ACTUALMATURITY,
                                                         'yyyyMMdd'))) >
                    TO_DATE(from_unixtime(unix_timestamp(D_DATADATE,
                                                         'yyyyMMdd'))) and
                    bc.CORPUSPAYMETHOD in ('080', '090') and
                    nvl(bd.OVERDUEBALANCE, 0) > 0 then
                '0'
               else
                round(
                      (nvl(bd.OVERDUEBALANCE, 0) + nvl(bd.DULLBALANCE, 0) +
                      nvl(bd.BADBALANCE, 0)),
                      2)
             END,
             0) +		
                nvl(bd.INTERESTBALANCE1, 0) +
                nvl(bd.INTERESTBALANCE2, 0) +
                nvl(bd.FINEBALANCE1, 0) + nvl(bd.FINEBALANCE2, 0)) > 0 then
            greatest(nvl(ljqs.leijqxqs, 0), 1)
           ELSE
            0
          END AS CUMULATE_TERM_NUM,  --累计欠款期数*/
		 CASE WHEN 	 
                        (nvl(round(
                                  (nvl(bd.OVERDUEBALANCE, 0) + nvl(bd.DULLBALANCE, 0) +	
                                  nvl(bd.BADBALANCE, 0)),
                                  2), 0) + --应有欠本金额，20221229添加
                    nvl(bd.INTERESTBALANCE1, 0) +
                    nvl(bd.INTERESTBALANCE2, 0) + nvl(bd.FINEBALANCE1, 0) +
                    nvl(bd.FINEBALANCE2, 0)) > 0 then
                greatest(nvl(ljqs.leijqxqs, 0), 1)
               ELSE
                0
             END  AS CUMULATE_TERM_NUM,  --累计欠款期数 SPE-20240813-0067 修改欠本金额和欠本日期逻辑
          -- NVL(HK.ZONGQISH,1) AS CURRENT_TERM_NUM,  --当前还款期数
		   NVL(HK.benqqish,1) AS CURRENT_TERM_NUM,  --当前还款期数 by qzj 20240116 取当期期数
          CODE.BANKNO AS CURR_CD,  --币种
          COALESCE(BD.MFCUSTOMERID,CI.MFCUSTOMERID,KD.KEHUHAOO) AS CUST_ID,  --客户号 20240709
          --按照客户信息表优先取核心客户号  by wzm 2024-6-13 09:41:37 影响金数存量委托贷款证件号码取值
          --COALESCE(CI.MFCUSTOMERID,CI.CUSTOMERID,KD.KEHUHAOO) AS CUST_ID,  --客户号
          TO_CHAR(D_DATADATE,'YYYYMMDD') AS DATA_DATE,  --数据日期
          'CL_1' AS DATE_SOURCESD,  --数据来源
          TRIM(CASE 
		       --WHEN CUST_INFO.CUSTOMERSCALE='022' THEN 'xwdk_dg'	--by:yxy 20240809 逻辑来源于信贷,应业务核对数据要求,从信贷段分离普惠对公数据
		       WHEN BD.SERIALNO='X2023033000011' THEN  'gjyw'
               WHEN SUBSTR(BD.BUSINESSTYPE,1,4) IN ('1070','1080','2050','2020','2022','2030','2040') THEN 'gjyw'
               WHEN BD.BUSINESSTYPE='2090030' THEN 'gjyw'
               WHEN SUBSTR(BD.BUSINESSTYPE,1,4) IN ('1090','2120') THEN 'gyl'
               WHEN BD.BUSINESSTYPE IN ('1010070','1010071','1010072') THEN 'gyl'
               WHEN BD.BUSINESSTYPE IN ('1060','1058010') AND CODE.BANKNO = 'CNY' THEN 'ytdk'
               WHEN BD.BUSINESSTYPE IN ('1060','1058010') AND CODE.BANKNO <> 'CNY' THEN 'gjyw'
           ELSE 'dgdk' END) AS DEPARTMENTD,  --归属部门
          --ELSE 'jtkh' END) AS DEPARTMENTD,  --归属部门
          BD.BUSINESSSUM AS DRAWDOWN_AMT,  --放款金额
         -- BP.BASERATE AS DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
          case  WHEN BC.ICTYPE IN ('020','150') THEN ''
 			ELSE BP.BASERATE end AS DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
          TO_DATE(BD.PUTOUTDATE) AS DRAWDOWN_DT,  --放款日期
         -- BP.BUSINESSRATE AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
         --原金数取 BP.BUSINESSRATE2
          BP.BUSINESSRATE2 AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
          CASE WHEN BP.PAYMENTTYPE='1' THEN 'B'
               WHEN BP.PAYMENTTYPE='2' THEN 'A'
               ELSE 'A'
          END AS DRAWDOWN_TYPE,  --放款方式
          CASE WHEN BC.OCCURTYPE ='015' THEN '展期'
               WHEN BC.OCCURTYPE ='060' THEN '收回再贷'
               WHEN BC.OCCURTYPE ='080' THEN '续授信'
               WHEN BC.OCCURTYPE ='085' THEN '变更批复'
          END AS DRAWDOWN_TYPE_NEW,  --发放类型
          NVL(REPLACE(UPPER(BPH.CODE),'C',''),BC.MANAGEUSERID) AS EMP_ID,  --信贷员员工号
          CASE WHEN BP.PAYMENTTYPE='1' THEN BP.BUSINESSSUM END AS ENTRUST_PAY_AMT,  --受托支付金额
          CASE WHEN BD.EXTENDTIMES <>0 THEN 'Y' ELSE 'N' END AS EXTENDTERM_FLG,  --展期标志
          TO_DATE(BD.FINISHDATE) AS FINISH_DT,  --结清日期
          CASE WHEN BC.BASERATETYPE ='5' THEN 'A'
               WHEN BC.BASERATETYPE='010' THEN 'B'
               ELSE 'C'
          END AS FLOAT_TYPE,  --参考利率类型
		  /*
          CASE WHEN P.JJH IS NOT null THEN 'O'
               ELSE 'I'
          END AS FUND_USE_LOC_CD,  --贷款资金使用位置
		  */
          CASE WHEN T7.COUNTRYCODE <> 'CHN' THEN 'O'
               ELSE 'I'
          END AS FUND_USE_LOC_CD,  --贷款资金使用位置	BY:YXY 20240322
          CASE WHEN (T7.ORGTYPE is null AND B2.BALANCESUM<=5000000)OR(NVL(T7.SCOPE,'5') IN ('4','5') AND AG.SERIALNO IS NOT NULL AND B2.BALANCESUM<=10000000) THEN 'Y' ELSE 'N' END AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
          I9.ECL_FINAL AS SP_PROV_AMT, --一般准备
          CASE WHEN CSP.SECTIONTYPE='95' THEN 'Y' ELSE 'N' END AS GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
          --CASE WHEN NVL(BC.GREENLOANPURPOSE,'0')<>'0'  THEN BD.BALANCE END AS GREEN_BALANCE,  --绿色信贷余额
          CASE WHEN NVL(BC.GREENLOANPURPOSE,'0')<>'0' THEN 'Y' ELSE 'N' END AS GREEN_LOAN_FLG,  --绿色贷款标志
          M.NEW_VALUES AS GREEN_LOAN_TYPE,  --绿色贷款用途分类
 /*         CASE WHEN GC.GUARANTYTYPE_NUMS > 1 THEN 'E'
          	   WHEN GC.GUARANTYTYPE ='60' THEN 'A'
               WHEN GC.GUARANTYTYPE ='50' AND GC.GUARANTYTYPE_GI LIKE'3%' THEN 'B01'
               WHEN GC.GUARANTYTYPE ='50' AND GC.GUARANTYTYPE_GI NOT LIKE'3%' THEN 'B99'
               ELSE 'C99'
          END AS GUARANTY_TYP,  --贷款担保方式*/
           TRIM(
		   case when dblxs=1 then 
           case when GC.VouchType like "040%" then "A" 							--A-质押
			 when GC.VouchType like "010%" or GC.VouchType="050" then "C99" 			--C99-保证
			 when fdcdys>0 and BCVouchtype like "02010%" then "B01"			--B01-房地产抵押
			 when fdcdys>0 and qtdbs>0 then "E01" 						--E01-房地产组合担保
			 when GC.VouchType like "020%" then "B99" end							--B99-其他抵押
			 when dblxs=0 then  "D" 											--D-信用
			 when dblxs>1 then 
			 case when fdcdys>0 and BCVouchtype like "02010%" then "B01" 		--B01-房地产抵押
			 when fdcdys>0 and qtdbs>0 then "E01" 						--"E01-房地产组合担保"
   	 	   else "E" end 														--E-组合
         end
		   )   AS GUARANTY_TYP,  --贷款担保方式
          CASE WHEN BP.PAYMENTTYPE<>'1' THEN BP.BUSINESSSUM END AS INDEPENDENCE_PAY_AMT,  --自主支付金额
		  /*
		  CASE
			WHEN BC.OURROLE <= 'A0960' AND BC.OURROLE >= 'A0791' THEN '1'	--节能环保产业
			WHEN BC.OURROLE <= 'A0091' AND BC.OURROLE >= 'A0005' THEN '2'	--新一代信息技术
			WHEN BC.OURROLE <= 'A0611' AND BC.OURROLE >= 'A0526' THEN '3'	--生物产业
			WHEN BC.OURROLE <= 'A0207' AND BC.OURROLE >= 'A0097' THEN '4'	--高端装备制造产业
			WHEN BC.OURROLE <= 'A0779' AND BC.OURROLE >= 'A0658' THEN '5'	--新能源产业
			WHEN BC.OURROLE <= 'A0521' AND BC.OURROLE >= 'A0216' THEN '6'	--新材料产业
			WHEN BC.OURROLE <= 'A0653' AND BC.OURROLE >= 'A0615' THEN '7'	--新能源汽车产业
			WHEN BC.OURROLE <= 'A1001' AND BC.OURROLE >= 'A0968' THEN '8'	--数字创意产业
			WHEN BC.OURROLE <= 'A1049' AND BC.OURROLE >= 'A1013' THEN '9'	--相关服务业
			ELSE NULL
		  END AS INDUST_STG_TYPE,	--战略新兴产业类型 BY:YXY 20240322
		  */
		  CASE
			WHEN BC.OURROLE IS NOT NULL AND BC.OURROLE NOT IN ('A0291','A4099','A9099')
				THEN (
					SELECT
						CASE
							WHEN CL.ITEMDESCRIBE LIKE '7%' THEN '1'	--节能环保产业
							WHEN CL.ITEMDESCRIBE LIKE '1%' THEN '2'	--新一代信息技术
							WHEN CL.ITEMDESCRIBE LIKE '4%' THEN '3'	--生物产业
							WHEN CL.ITEMDESCRIBE LIKE '2%' THEN '4'	--高端装备制造产业
							WHEN CL.ITEMDESCRIBE LIKE '6%' THEN '5'	--新能源产业
							WHEN CL.ITEMDESCRIBE LIKE '3%' THEN '6'	--新材料产业
							WHEN CL.ITEMDESCRIBE LIKE '5%' THEN '7'	--新能源汽车产业
							WHEN CL.ITEMDESCRIBE LIKE '8%' THEN '8'	--数字创意产业
							WHEN CL.ITEMDESCRIBE LIKE '9%' THEN '9'	--相关服务业
							ELSE NULL
						END
					FROM OMI.CL_CODE_LIBRARY_HS CL
					WHERE CL.PARTID = V_PARTID 
					  AND CL.BEGNDT <= D_DATADATE
					  AND CL.OVERDT > D_DATADATE
					  AND CL.ITEMNO = BC.OURROLE
				)
		  END AS INDUST_STG_TYPE,	--战略新兴产业类型 BY:YXY 20250116
          CASE WHEN SUBSTR(BC.DIRECTION,2,4) IN('2710','2720','2730','2740','2750','2761','2762','2770','2780','3741','3742','3743','3744','3749','4343','3562','3563','3569','3832','3833','3841','3921','3922','3940','3931','3932','3933','3934','3939','3951','3952','3953','3971','3972','3973','3974','3975','3976','3979','3981','3982','3983','3984','3985','3989','3961','3962','3963','3969','3990','3911','3912','3913','3914','3915','3919','3474','3475','3581','3582','3583','3584','3585','3586','3589','4011','4012','4013','4014','4015','4016','4019','4021','4022','4023','4024','4025','4026','4027','4028','4029','4040','4090','2664','2665') THEN '1' ELSE '2' END AS INDUST_TRAN_FLG,  --工业企业技术改造升级标识
		  /*
		  case when substr(replace(BC.DIRECTION,'*',''),1,3) in ('A05','B06','B07','B08','B09','B10','B11','B12','C13','C14','C15','C16','C17','C18','C19','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31','C32','C33','C34','C35','C36','C37','C38','C39','C40','C41','C42','C43','D44','D45','D46','E47','E48','E49','E50','F51','F52','G53','G54','G55','G56','G57','G58','G60','H61','H62','I63','I64','I65','J66','J67','J68','J69','L71','L72','M73','M74','M75','N76','N77','N78','N79','O80','O81','O82','Q84','Q85','R86','R87','R88','R89','R90')
		  or replace(BC.DIRECTION,'*','') like 'K%'
		  or replace(BC.DIRECTION,'*','') like 'P%' then '1'
		  else '2' END AS INDUST_TRAN_FLG,  --工业企业技术改造升级标识  by cwx 20231226
		  */	--by:yxy 20240515 核查数据发现之前的逻辑是核对正常的
          'N' AS INTERNET_LOAN_FLG,  --互联网贷款标志
          trim(CASE WHEN BD.Businesstype LIKE "1130%" THEN 'F'
          	   WHEN BC.ICTYPE IN ('020','150') THEN 'F'
               WHEN BC.ICTYPE ='90' THEN 'L0'
               WHEN BC.ICTYPE ='30' THEN 'L2'
               WHEN BC.ICTYPE ='40' THEN 'L3'
               WHEN BC.ICTYPE ='50' THEN 'L4'
               WHEN BC.ICTYPE IN ('70','80','130','140') THEN 'L5'
               ELSE 'L9'
          END) AS INT_RATE_TYP,  --利率类型
          CASE WHEN BP.ICCYC = '010' THEN '03'
               WHEN BP.ICCYC = '020' THEN '04'
               WHEN BP.ICCYC = '025' THEN '05'
               WHEN BP.ICCYC = '030' THEN '06'
               WHEN BP.ICCYC = '040' THEN '07'
               WHEN BP.ICCYC = '050' THEN  '99'
               ELSE '03'
          END AS INT_REPAY_FREQ,  --利息还款频率
          CASE WHEN BP.ICCYC = '050' THEN '手工计息' END AS INT_REPAY_FREQ_DESC,  --利息还款频率说明
          CASE WHEN BD.PUTOUTDATE=T7.CREDITDATE AND BD.SERIALNO= (SELECT MIN(SERIALNO) FROM OMI.CL_BUSINESS_DUEBILL_HS WHERE BEGNDT <= D_DATADATE AND OVERDT > D_DATADATE AND PARTID = V_PARTID) THEN 'Y' ELSE 'N' END AS IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
          --CASE WHEN BD.PUTOUTDATE=T7.CREDITDATE AND BD.SERIALNO= MIN_SERIALNO THEN 'Y' ELSE 'N' END AS IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
          CASE WHEN BC.ICTYPE IN ('020','150') THEN 'Y'
               ELSE 'N'
          END AS IS_REPAY_OPTIONS,  --是否内嵌提前还款权
          --KY.YESHUX09 AS ITEM_CD,  --科目号
		 -- CASE 
         --  WHEN TRN.INR IS NOT NULL THEN '11420101'
         --  WHEN ky.yeshux09='BM_HS113201cdjj2' THEN '11320101'  --11420101  ->11320101  by 20230428
         --      ELSE --nvl(ky.yeshux09,'11420101') 
         --      ky.yeshux09
        --       END AS ITEM_CD,  --科目号  20231011 by qzj 和老east同逻辑
		  CASE 
           WHEN TRN.INR IS NOT NULL THEN '11420101'
         --  WHEN ky.yeshux09='BM_HS113201cdjj2' THEN '11320101'  --11420101  ->11320101  by 20230428
               ELSE --nvl(ky.yeshux09,'11420101') 
              nvl(nbhs.kemuhaoo,ky.yeshux09)
               END AS ITEM_CD,  --科目号  20240515 by qzj 和老east同逻辑  dgxdfhz eg:解决科目号为BM_HS115208cdjj2的问题 取11520801
          CASE WHEN (NVL(BD.INTERESTBALANCE1,0)+NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE1,0)+NVL(BD.FINEBALANCE2,0))>0 THEN TO_DATE(NVL(BD.INTDATE,BD.ACTUALMATURITY)) ELSE '' END AS I_OD_DT,  --利息逾期日期
          CASE WHEN SUBSTR(BC.DIRECTION,2,4) IN('8610','8622','8710','8720','8740','6421','6429','8621','8623','8624','8625','8626','8629','8730','8770','8810','8870','8890','6572','6422','8831','8832','8840','8850','8860','2431','2432','2433','2434','2435','2436','2437','2438','2439','3075','3076','7251','7259','7491','7492','5143','5144','5145','5243','5244','7124','7125','6321','6322','6331','8750','8760','8820','5183','5184','5146','5245','5246','9011','9012','9013','9019','9020','9090','7850','7861','7862','7869','7712','7715','7716','9030','5622','2222','2642','2644','2664','2311','2312','2319','2320','2330','8060','7281','7284','7289','9051','9053','9059','7298','7121','7123','7350','8393','3542','3474','3931','3932','3933','3934','3939','5178','3471','3953','3472','3473','5248','3873','2461','2462','2469','2421','2422','2423','2429','5147','5247','2411','5141','5241','2412','2414','2451','2456','2459','2672','3951','3952','5137','5271','5149','5249') THEN 'Y' ELSE 'N' END AS LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
          BD.BALANCE AS LOAN_ACCT_BAL,  --贷款余额
          coalesce(KJ.JIGOUZWM,KKD.DFZHKHHM,JG2.ORG_NAM,LJG.ORG_NAM) AS LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
          COALESCE(KH.KEHUZHMC,H.ZHANGHMC,KK.KEHUZHMC) AS LOAN_ACCT_NAME,  --贷款入账账户名称
          --NVL(KF.DKRZHZHH,GLE2.ACT) AS LOAN_ACCT_NUM,  --贷款入账账号
		  CASE WHEN BT.TYPENAME LIKE '%福费廷%' THEN NVL(GLE2.ACT,(CASE WHEN
                HB.ZHANGHAO IS NULL THEN KF.DKRZHZHH
               WHEN KKD.DFZHANGH IS NOT NULL THEN KKD.DFZHANGH
               ELSE
               KF.DKRZHZHH END)) 
               ELSE 
          NVL((CASE WHEN HB.ZHANGHAO IS NULL THEN KF.DKRZHZHH
                    WHEN KKD.DFZHANGH IS NOT NULL THEN KKD.DFZHANGH
                    ELSE KF.DKRZHZHH
          END), GLE2.ACT) END AS LOAN_ACCT_NUM,  --贷款入账账号
          /*CASE WHEN BC.BUSINESSTYPE ='1030' THEN '1'
               WHEN BC.BUSINESSTYPE IN ('1010','1010010','101001915') THEN '2'
               WHEN BC.BUSINESSTYPE LIKE '10300%' THEN '3'
               WHEN BC.BUSINESSTYPE=1040050 THEN '5'
               WHEN BC.BUSINESSTYPE LIKE '1058%' THEN '6'
               WHEN BC.BUSINESSTYPE=2070 THEN '7'
               ELSE '9'
          END AS LOAN_BUSINESS_TYP,*/  --贷款用途分类
          (case when bd.SerialNo in(select DuebillNo from omi.cl_advanced_info_hs where begndt <= D_DATADATE and overdt > D_DATADATE and partid = V_PARTID) then '9' 
                  else (case when BC.BUSINESSTYPE in ('101001934','1062','1010010','1010020','101019032','101019030','101001932','101001940','101001942','101001931',
                                                      '101001930','1010012','1010112','1010150','1010100','1010090','1010120','1010080','1010160','1010130',
                                                      '1010110','101001936','101019034','1010070','1010140','1010085','101019036') THEN '2'
                             WHEN BC.BUSINESSTYPE in ('1030010','1030020') THEN '3'
                             WHEN BC.BUSINESSTYPE in ('1010050','1040010','1050010','1050020') THEN '1'
                             WHEN BC.BUSINESSTYPE in ('101019036','1040030','1058010','1058020','1060') THEN '4'
                             else '4' end) 
             end) AS LOAN_BUSINESS_TYP,  --贷款用途分类 cqcs 20240913
          'N' AS LOAN_BUY_INT,  --是否转入贷款
          -- NVL(HK.HUANKZHH,BD.SERIALNO) AS LOAN_FHZ_NUM,  --贷款分户账账号
          NVL(kd.dkzhangh,BD.SERIALNO) AS LOAN_FHZ_NUM,  --贷款分户账账号 by qzj 20240202 同生产east dgxdywjjb逻辑 取贷款分户账号字段
          TRIM(CASE WHEN BC.CLASSIFYRESULT= '01' THEN '1'
               WHEN BC.CLASSIFYRESULT= '02' THEN '2'
               WHEN BC.CLASSIFYRESULT= '03' THEN '3'
               WHEN BC.CLASSIFYRESULT= '04' THEN '4'
               WHEN BC.CLASSIFYRESULT= '05' THEN '5'
               WHEN BC.CLASSIFYRESULT IS NULL THEN '1'
          END) AS LOAN_GRADE_CD,  --五级分类代码
          CASE WHEN BC.OCCURTYPE ='010' THEN '1'
               WHEN BC.OCCURTYPE ='060' THEN '2'
               WHEN BC.OCCURTYPE ='020' THEN '3'
               WHEN BC.OCCURTYPE IN ('015','030','040','050','080','085','090') THEN '9'
               WHEN BC.OCCURTYPE ='095' THEN '91'
               ELSE '1'
          END AS LOAN_KIND_CD,  --贷款形式
          BD.SERIALNO AS LOAN_NUM,  --贷款编号
          CASE WHEN BC.OCCURTYPE IN ('020','060','090') THEN BP.MOSTLYDUEBILLNO ELSE '' END AS LOAN_NUM_OLD,  --原贷款编号
          KJ4.DIQDAIMA AS LOAN_PURPOSE_AREA,  --贷款投向地区
          --replace(BC.DIRECTION,'*','') AS LOAN_PURPOSE_CD,  --贷款投向   by qzj 20231115 去除贷款投向中的*
		  nvl(replace(BC.DIRECTION,'*',''),COALESCE(XDKH.INDUSTRYTYPE,EII.INDUSTRYTYPE,KC.HYDMXLEI,KC.HANGYEDM)) AS LOAN_PURPOSE_CD,  --贷款投向 BY:YXY 20241023 上游数据有贷款投向行业为空的情况出现,和业务沟通修改,当合同表中的贷款投向行业为空时,取客户所在行业
		  'CHN' AS LOAN_PURPOSE_COUNTRY_NEW,--贷款投向国别
          BC.BUSINESSPROP AS LOAN_RATIO,  --出资比例
          --CASE WHEN BC.OCCURTYPE ='095' THEN TO_DATE(BD.PUTOUTDATE) END AS LOAN_RESCHED_DT,  --贷款重组日期  
          CASE WHEN KZ.CHULIZHT='1' THEN 'Y' ELSE 'N' END AS LOAN_SELL_INT,  --转出标志
          'N' AS LOW_RISK_FLAG,  --是否低风险业务
          CASE WHEN SUBSTR(BC.VOUCHTYPE,0,3) IN ('040','050') THEN '0'
               WHEN BC.VOUCHTYPE LIKE '020%' THEN '1'
               WHEN SUBSTR(BC.VOUCHTYPE,0,3) IN ('010','070') THEN '2'
               WHEN BC.VOUCHTYPE ='005' THEN '3'
          END AS MAIN_GUARANTY_TYP,  --主要担保方式
          CASE WHEN BD.BUSINESSTYPE LIKE '1130%' THEN ''
               WHEN BD.EXTENDTIMES>0 THEN /*TO_DATE(BD.MATURITY)*/
               TO_DATE(BE.MATURITY) --MOD BY ZWK
               ELSE TO_DATE(BD.ACTUALMATURITY)
          END AS MATURITY_DT,  --原始到期日期
          KX.S1 AS YEAR_INT_INCOM,  --本年利息收入
          TO_DATE(KDZ.DAOQRIQI) AS NEXT_INT_PAY_DT,  --下一付息日
         /*CASE WHEN TT.SCTXRIQI IS NOT NULL THEN TDH_TODATE(TT.SCTXRIQI)
               WHEN TT.SCTXRIQI IS NULL AND TT.SCYQTXRQ IS NOT NULL THEN TDH_TODATE(TT.SCYQTXRQ)
               WHEN TT.SCTXRIQI IS NULL AND TT.SCYQTXRQ IS NULL AND TT.SCNYTXRQ IS NOT NULL THEN TDH_TODATE(TT.SCNYTXRQ)
               ELSE (CASE WHEN BD.EXTENDTIMES>0 THEN TDH_TODATE(BD.MATURITY) 
                          ELSE TDH_TODATE(BD.ACTUALMATURITY)
                     END)
          END AS NEXT_REPRICING_DT,  --下一利率重定价日*/
          --由上次调息日改为下次调息日 薄天开要求 by wzm 2024-4-23 17:32:25
          CASE WHEN TT.XCTXRIQI IS NOT NULL THEN TDH_TODATE(TT.XCTXRIQI)
               WHEN TT.XCTXRIQI IS NULL AND TT.XCYQTXRQ IS NOT NULL THEN TDH_TODATE(TT.XCYQTXRQ)
               WHEN TT.XCTXRIQI IS NULL AND TT.XCYQTXRQ IS NULL AND TT.XCNYTXRQ IS NOT NULL THEN TDH_TODATE(TT.XCNYTXRQ)
               ELSE (CASE WHEN BD.EXTENDTIMES>0 THEN TDH_TODATE(BD.MATURITY) 
                          ELSE TDH_TODATE(BD.ACTUALMATURITY)
                     END)
          END AS NEXT_REPRICING_DT,
          --和李曜确认优先取信贷逻辑
--        CASE   WHEN BD.EXTENDTIMES>0 THEN TDH_TODATE(BD.MATURITY) 
--               WHEN TT.SCTXRIQI IS NOT NULL THEN TDH_TODATE(TT.SCTXRIQI)
--               WHEN TT.SCTXRIQI IS NULL AND TT.SCYQTXRQ IS NOT NULL THEN TDH_TODATE(TT.SCYQTXRQ)
--               WHEN TT.SCTXRIQI IS NULL AND TT.SCYQTXRQ IS NULL AND TT.SCNYTXRQ IS NOT NULL THEN TDH_TODATE(TT.SCNYTXRQ)
--               ELSE  TDH_TODATE(BD.ACTUALMATURITY)                 
--          END AS NEXT_REPRICING_DT,  --下一利率重定价日
         --G33还没测试，有影响再调整， wzm 2023年11月21日10:16:21 和张静祎确认建议按照第一段逻辑取值
        --  TDH_TODATE(BD.ACTUALMATURITY)  AS NEXT_REPRICING_DT,  --下一利率重定价日
          
          --DATEDIFF(D_DATADATE, LEAST(NVL(BD.OVERDUEDATE, D_DATADATE), NVL(BD.INTDATE, D_DATADATE))) by gjw 20231018
		  /*DATEDIFF(D_DATADATE, LEAST(NVL(BD.OVERDUEDATE, CASE WHEN NVL(BD.INTERESTBALANCE1, 0) + NVL(BD.INTERESTBALANCE2, 0) +NVL(BD.FINEBALANCE1, 0) + NVL(BD.FINEBALANCE2, 0) > 0 THEN BD.INTDATE END), 
          NVL(CASE WHEN NVL(BD.INTERESTBALANCE1, 0) + NVL(BD.INTERESTBALANCE2, 0) +NVL(BD.FINEBALANCE1, 0) + NVL(BD.FINEBALANCE2, 0) > 0 THEN BD.INTDATE END,BD.OVERDUEDATE)))+1 AS OD_DAYS,  --逾期天数*/
          /*
		  GREATEST(CASE WHEN BD.BUSINESSTYPE IN('1061','1040030','1010050','1040010') AND BD.INTDATE IS NOT NULL AND (NVL(BD.OVERDUEBALANCE,0)+NVL(BD.DULLBALANCE,0)+NVL(BD.BADBALANCE,0))>0 THEN DATEDIFF(D_DATADATE,BD.INTDATE)+1 
                        WHEN BD.ACTUALMATURITY<TO_CHAR(D_DATADATE,'YYYYMMDD') AND (NVL(BD.OVERDUEBALANCE,0)+NVL(BD.DULLBALANCE,0)+NVL(BD.BADBALANCE,0))>0 
                        THEN DATEDIFF(D_DATADATE,BD.ACTUALMATURITY)+1
                        ELSE 0 END,CASE WHEN (NVL(BD.INTERESTBALANCE1,0)+NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE1,0)+NVL(BD.FINEBALANCE2,0))>0 THEN DATEDIFF(D_DATADATE,BD.INTDATE)+1 ELSE 0 END
          ) AS OD_DAYS,  --逾期天数
		  */
		  GREATEST(NVL(DATEDIFF(D_DATADATE,TO_DATE(BD.OVERDUEDATE))+1,0),
			NVL(DATEDIFF(D_DATADATE,TO_DATE(CASE WHEN (NVL(BD.INTERESTBALANCE1,0)+NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE1,0)+NVL(BD.FINEBALANCE2,0))>0 THEN TO_DATE(NVL(BD.INTDATE,BD.ACTUALMATURITY)) ELSE NULL END))+1,0)
		  ) AS OD_DAYS,  --逾期天数  --BY:YXY 20240926 根据EAST修改,未和信贷进行沟通确认,可能会再次修改逻辑
          CASE WHEN NVL(BD.OVERDUEBALANCE,0)+NVL(BD.INTERESTBALANCE1,0)+NVL(BD.FINEBALANCE1,0)+NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE2,0)>0 THEN 'Y' 
               ELSE 'N'
          END AS OD_FLG,  --逾期标志
          NVL(BD.INTERESTBALANCE1,0)+NVL(BD.FINEBALANCE1,0) AS OD_INT,  --逾期利息
          /*CASE WHEN 
               CASE WHEN BD.BusinessType in('1061','1040030','1010050','1040010') and BD.intdate is not null and (nvl(BD.OverdueBalance,0)+nvl(BD.DullBalance,0)+nvl(BD.BadBalance,0))>0 
                    THEN DATEDIFF(TO_DATE(D_DATADATE),to_date(REPLACE(BD.intdate,'/','-'))) --若本金逾期且欠息日非空，则欠息日到当前日期之间的天数为本金逾期天数。
          WHEN to_date(REPLACE(BD.ActualMaturity,'/','-'))<to_date(D_DATADATE) and (nvl(BD.OverdueBalance,0)+nvl(BD.DullBalance,0)+nvl(BD.BadBalance,0))>0 
          THEN datediff(to_date(D_DATADATE),to_date(REPLACE(BD.ActualMaturity,'/','-'))) 
          ELSE 0 END < 1 AND (NVL(BD.OVERDUEBALANCE,0)+(NVL(BD.DULLBALANCE,0)+NVL(BD.BADBALANCE,0))) > 0 THEN 
          (NVL(BD.OVERDUEBALANCE,0)+(NVL(BD.DULLBALANCE,0)+NVL(BD.BADBALANCE,0)))+NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE2,0)
          ELSE NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE2,0)+NVL(BD.INTERESTBALANCE1,0)+NVL(BD.FINEBALANCE1,0) END AS OD_INT,*/  --逾期利息 --by gjw 20240304 根据生产目前上报口径，有逾期金额，但是没有本金逾期天数的算到了逾期利息   by qzj 20240423 最后的ELSE改为表内欠息余额+本金罚息+表外欠息余额+利息罚息
          NVL(BD.INTERESTBALANCE2,0)+NVL(BD.FINEBALANCE2,0) AS OD_INT_OBS,  --表外欠息
          /*CASE WHEN BC.BUSINESSTYPE NOT IN ('1040050','1040010') AND TO_DATE(BD.ACTUALMATURITY)>D_DATADATE AND BC.CORPUSPAYMETHOD IN ('080','090') AND NVL(BD.OVERDUEBALANCE,0)>0 THEN '0'
               ELSE ROUND((NVL(BD.OVERDUEBALANCE,0)+(NVL(BD.DULLBALANCE,0)+NVL(BD.BADBALANCE,0))),2)
          END AS OD_LOAN_ACCT_BAL,*/  --逾期贷款余额  改下面的会影响1104很多表
		   nvl( round(
                          (nvl(bd.OVERDUEBALANCE, 0) + nvl(bd.DULLBALANCE, 0) +		--SPE-20240813-0067 修改欠本金额和欠本日期逻辑
                          nvl(bd.BADBALANCE, 0)),  
                          2),
                 0) AS OD_LOAN_ACCT_BAL, --逾期贷款余额 SPE-20240813-0067 修改欠本金额和欠本日期逻辑
          /*CASE WHEN 
               CASE WHEN BD.BusinessType in('1061','1040030','1010050','1040010') and BD.intdate is not null and (nvl(BD.OverdueBalance,0)+nvl(BD.DullBalance,0)+nvl(BD.BadBalance,0))>0 
                    THEN DATEDIFF(TO_DATE(D_DATADATE),to_date(REPLACE(BD.intdate,'/','-'))) --若本金逾期且欠息日非空，则欠息日到当前日期之间的天数为本金逾期天数。
          WHEN to_date(REPLACE(BD.ActualMaturity,'/','-'))<to_date(D_DATADATE) and (nvl(BD.OverdueBalance,0)+nvl(BD.DullBalance,0)+nvl(BD.BadBalance,0))>0 
          THEN datediff(to_date(D_DATADATE),to_date(REPLACE(BD.ActualMaturity,'/','-'))) 
          ELSE 0 END > 0 THEN 
          CASE WHEN BC.BUSINESSTYPE NOT IN ('1040050','1040010') AND TO_DATE(BD.ACTUALMATURITY)>D_DATADATE AND BC.CORPUSPAYMETHOD IN ('080','090') AND NVL(BD.OVERDUEBALANCE,0)>0 THEN '0'
          ELSE ROUND((NVL(BD.OVERDUEBALANCE,0)+(NVL(BD.DULLBALANCE,0)+NVL(BD.BADBALANCE,0))),2)
          END
          ELSE 0 END AS OD_LOAN_ACCT_BAL,*/ --逾期贷款余额 --by gjw 20240304 根据生产目前上报口径，得先判断有本金逾期天数，再取逾期余额
          CASE WHEN BD.MFORGID='0090102' THEN '0090101' 
               ELSE BD.MFORGID
          END AS ORG_NUM,  --机构号
          NVL(adv.DRAFT_NUMBER,BDOLD.RelativeSerialNo2)  AS ORIG_ACCT_NO,   --原账号
		  --CASE WHEN AI.DUEBILLNO is not null then adv.DRAFT_NUMBER else BDOLD.RelativeSerialNo2 END AS ORIG_ACCT_NO,   --原账号
          --BC.TERMMONTH AS ORIG_TERM,  --原始期限
		  --CEIL(MONTHS_BETWEEN(TO_DATE(BP.MATURITY),TO_DATE(BP.PUTOUTDATE))) AS ORIG_TERM,  --原始期限
		  --datediff(TO_DATE(BP.MATURITY),TO_DATE(BP.PUTOUTDATE)) AS ORIG_TERM,  --原始期限  MONTHS_BETWEEN对31天的月份有误差，调整日期差值逻辑 modify by  cwx 20231207
          datediff(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE)) AS ORIG_TERM,  --原始期限  MONTHS_BETWEEN对31天的月份有误差，调整日期差值逻辑 modify by  cwx 20231207
          'D' AS ORIG_TERM_TYPE,  --原始期限类型
          --'N' AS OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
		  CASE WHEN M1.NEW_VALUES = '0203' AND T7.COUNTRYCODE <> 'CHN' THEN 'Y'
			   ELSE 'N'
		  END AS OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志 BY:YXY 20240328 根据资金使用位置和账户类型判断是否为境外并购贷款
          COALESCE(KJ2.JIGOUZWM,KJ3.JIGOUZWM,JG2.ORG_NAM) AS PAY_ACCT_BANK,  --还款账号所属行名称
          NVL(HK.HUANKZHH,GLE2.ACT) AS PAY_ACCT_NUM,  --还款账号
          BC.ISFTZ AS PFTZ_LOAN_FLG,  --自贸区贷款标志
          'N' AS POVERTY_ALLE,  --已脱贫人口贷款标志
          --TT.LILVLEIX AS PRICING_BASE_TYPE,  --定价基础类型
          TRIM(CASE WHEN BC.BASERATETYPE='005' THEN 'C' --贷款市场报价利率(LPR)
               WHEN BC.BASERATETYPE='010' THEN 'B02' --人行基准利率
               WHEN BC.BASERATETYPE='040' THEN 'A0201' --伦敦同业拆借利率(Libor)
               WHEN BC.BASERATETYPE='050' THEN 'A0202' --香港同业拆借利率(Hibor)
               ELSE 'Z99' --公积金基准利率
		  END) AS PRICING_BASE_TYPE,  --定价基础类型 by qzj 20231122
          /*CASE WHEN BC.BUSINESSTYPE NOT IN ('1040050','1040010') AND TO_DATE(BD.ACTUALMATURITY)>D_DATADATE AND BC.CORPUSPAYMETHOD IN ('080','090')AND NVL(BD.OVERDUEBALANCE,0)>0 THEN '' 
               ELSE TO_DATE(BD.OVERDUEDATE)
          END AS P_OD_DT,*/  --本金逾期日期 同逾期金额
		  TO_DATE(BD.OVERDUEDATE) AS P_OD_DT,  --本金逾期日期 SPE-20240813-0067 修改欠本金额和欠本日期逻辑
          BD.BUSINESSRATE2 AS REAL_INT_RAT,  --实际利率
          CASE WHEN BC.OCCURTYPE ='090' THEN 'Y' 
               ELSE 'N'
          END AS RENEW_FLG,  --无还本续贷标志
          CASE WHEN BC.OCCURTYPE ='020' THEN 'Y' 
               ELSE 'N'
          END AS REPAY_FLG,  --借新还旧标志
          NVL(HK.ZONGQISH,1) AS REPAY_TERM_NUM,  --还款总期数
          CASE WHEN BD.ICCYC IN ('10','60') THEN '1'
               WHEN BD.ICCYC IN ('20','70') THEN '2'
               WHEN BD.ICCYC IN ('25','80') THEN'3'
               WHEN BD.ICCYC IN ('30','90') THEN '4'
               WHEN BD.ICCYC ='40' THEN '5'
               WHEN BP.CORPUSPAYMETHOD ='030' THEN '6'
               WHEN BP.CORPUSPAYMETHOD ='040' THEN '5'
               WHEN BP.CORPUSPAYMETHOD  IN ('010','020','090') THEN '7'
               -- WHEN BP.CORPUSPAYMETHOD IS NOT NULL THEN '6' 
               -- ELSE '7' --其他
                ELSE '6' --分期付息一次还本  BY QZJ 20240520
          END AS REPAY_TYP,  --还款方式_2
          CASE WHEN BP.CORPUSPAYMETHOD ='010' THEN '等额本金'
               WHEN BP.CORPUSPAYMETHOD ='020' THEN '等额本息'
               WHEN BP.CORPUSPAYMETHOD ='090' THEN '多次还息定制还本'
          END AS REPAY_TYP_DESC,  --还款方式说明
         -- CASE WHEN BC.OCCURTYPE ='095' THEN 'Y' 
          --     ELSE 'N'
         -- END AS RESCHED_FLG,  --重组标志
          --BD.BAILACOUNT AS SECURITY_ACCT_NUM,  --保证金账号
		  BP.Bailaccount AS SECURITY_ACCT_NUM,  --保证金账号 20240125 lx 修改 贸易融资表uat问题
          BD.BAILSUM AS SECURITY_AMT,  --保证金金额
          M2.NEW_VALUES AS SECURITY_CURR,  --保证金币种
          --BD.BAILPERCENT AS SECURITY_RATE,  --保证金比例 20240125 lx 修改 贸易融资表uat问题
		  CASE WHEN bc.Bailratio is NOT NULL THEN bc.Bailratio 
          ELSE '0' END AS SECURITY_RATE,  --保证金比例 
          'N' AS TAX_RELATED_FLG,  --银税合作贷款标志
          'N' AS TRANSFERS_LOAN_FLG,  --转贷款标志
          'N' AS UNDERTAK_GUAR_TYPE,  --创业担保贷款类型
           -- BC.PURPOSE AS USEOFUNDS,  --贷款用途
           case
                  when BD.BusinessType like '1130%' /*and businessType <>'1130020'*/
                   then
                   BCOLD.Purpose
                  else
                   BC.Purpose
                end AS USEOFUNDS,  --贷款用途  by qzj 20231204同生产east dgxdywjjb
          CASE WHEN SUBSTR(KJ.JIGOUHAO,0,3)  IN ('010','012','014') THEN 'Y' 
               ELSE 'N' 
          END AS YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志   
		  'CHN' AS LOAN_PURPOSE_COUNTRY,
          CASE WHEN HK.HUANKFSH = '1' THEN '07'
               WHEN HK.HUANKFSH = '2' AND HK.DZHHKJIH = '0' THEN '07'
               WHEN HK.HUANKFSH = '2' AND HK.DZHHKJIH = '1' THEN '99'
               WHEN HK.HUANKFSH IN ('3','4','5','6') AND HK.HKZHOUQI LIKE '1Q%' THEN '04'
               WHEN HK.HUANKFSH IN ('3','4','5','6') AND HK.HKZHOUQI LIKE '1Y%' THEN '06'
               WHEN HK.HUANKFSH IN ('3','4','5','6') AND HK.HKZHOUQI LIKE '1M%' THEN '03'
               WHEN HK.HUANKFSH IN ('3','4','5','6') AND HK.HKZHOUQI LIKE '6M%' THEN '05'
               WHEN HK.HUANKFSH IN ('3','4','5','6') AND HK.HKZHOUQI LIKE '1H%' THEN '05'
               ELSE '99'
	      END AS PPL_REPAY_FREQ,  --本金还款频率
	      0 AS INT_ADJEST_AMT,  --利息调整
		  CASE WHEN nbz.zhanghao IS NOT NULL OR nbz1.zhanghao IS NOT NULL  THEN    --by SPE20230817018 20230828
                bp.InsideAccountRemark||(CASE WHEN length(substr(hxbz.beizhuuu,instr(hxbz.beizhuuu,']',1,1)+1))=0 
                THEN NULL 
				ELSE substr(hxbz.beizhuuu,instr(hxbz.beizhuuu,']',1,1)+1) END)
                ELSE '' END AS REMARK  --备注
				,/*CASE
					WHEN BC.TERMMONTH = 12 AND CEIL(MONTHS_BETWEEN(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE))) = 12 AND datediff(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE)) = 366 THEN 0
					ELSE BC.TERMMONTH
				END AS ORIG_TERM_BC,*/  --原始期限		BY:YXY 20240326 按自然月份判断贷款期限，排除闰年影响，并取合同上的期限，如果为闰年366天，原始期限为12月，默认为0
		  CASE
					WHEN BD.EXTENDTIMES <>0 THEN NVL(BC.TERMMONTH,CEIL(MONTHS_BETWEEN(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE))))
					ELSE CEIL(MONTHS_BETWEEN(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE)))
				END AS ORIG_TERM_BC,  --原始期限		
				
          --金数委托贷款发生额流水号取值 by wzm 2024-4-24 17:12:43
		BW.SERIALNO AS DRAWDOWN_NUM, --放款编号
		case when BD.Businesstype LIKE  "1%" AND 
				BD.Businesstype NOT LIKE "1130%" AND 
				BD.Businesstype NOT LIKE "1020%"  AND 
				(NVL(t7.OrgType,"1")<>"400410") 
		THEN (CASE WHEN BC.VouchType LIKE "010%" THEN "C99"--保证
					  WHEN BC.VouchType LIKE "020%" THEN 
						   CASE WHEN BC.VouchType LIKE "02010%" THEN "B01" else "B99" end --抵押
					  WHEN BC.VouchType LIKE "040%" or BC.VouchType LIKE "050%" THEN "A" --质押
					  WHEN BC.VouchType LIKE "005%" THEN "D" --信用
		              ELSE "Z" END) 
		END AS guaranty_typ_js, --担保方式(金数担保物信息用)
		--客户号账号维度和客户维度客户号不一致问题,影响金数委托贷款取值 by  wzm 2024-7-16 09:22:11
	 COALESCE(CI.MFCUSTOMERID,CI.CUSTOMERID,KD.KEHUHAOO) AS CUST_ID_JS,  --客户号
	 trim(
        CASE
        WHEN BD.BUSINESSTYPE='1058010' THEN trim('F12')--银团贷款/并购贷款
        WHEN BD.BUSINESSTYPE='1058020' THEN trim('F12')--非银团贷款/并购贷款
        WHEN BD.BUSINESSTYPE='1080050' THEN 'F081'--出口T/T押汇/国际贸易融资
        WHEN BD.BUSINESSTYPE in('1080031','T1310012') THEN 'F081'--出口信用证押汇/国际贸易融资
        WHEN BD.BUSINESSTYPE in('1080090','1080072') THEN 'F081'--进口TT押汇/国际贸易融资
        WHEN BD.BUSINESSTYPE='1080071' THEN 'F081'--进口代收押汇/国际贸易融资
        WHEN BD.BUSINESSTYPE='1080020' THEN 'F081'--信用证项下打包贷款/国际贸易融资
        WHEN BD.BUSINESSTYPE='1080070' THEN 'F081'--信用证项下进口押汇/国际贸易融资
        WHEN BD.BUSINESSTYPE in('2050070','1080220') THEN 'F081'--进口代收项下代付/国际贸易融资
        WHEN BD.BUSINESSTYPE='1070060' THEN 'F082'--国内信用证自营福费廷/国内贸易融资
        WHEN BD.BUSINESSTYPE='1090040' THEN 'F082'--国内有追卖方保理/国内贸易融资
        WHEN BD.BUSINESSTYPE='1090050' THEN 'F082'--国内保理低风险融资/国内贸易融资
        WHEN BD.BUSINESSTYPE='1070040' THEN 'F082'--国内信用证买方押汇
        WHEN BD.BUSINESSTYPE='1090070' THEN 'F082'--买房联通信
        WHEN BD.BUSINESSTYPE='1010071' THEN 'F082'
        WHEN BD.BUSINESSTYPE='1080210' THEN 'F081'
        WHEN BD.BUSINESSTYPE='1090060' THEN 'F082'
        WHEN BD.BUSINESSTYPE='1010030' THEN 'F041'  --业务品种为法人账户透支
        WHEN BD.BUSINESSTYPE="1130010" THEN "F052"  --保函垫款
        WHEN BD.BUSINESSTYPE="1130060" THEN "F052"  --商票保证保函垫款
        WHEN BD.BUSINESSTYPE="1130050" THEN "F059"  --保理资管易垫款
		WHEN BD.BUSINESSTYPE="1130045" THEN "F059"  --买方连信通垫款
		WHEN BD.BUSINESSTYPE="1130080" THEN "F059"  --贴现垫款
		WHEN BD.BUSINESSTYPE="1130020" THEN "F051"  --承兑汇票垫款
		WHEN BD.BUSINESSTYPE="1130030" THEN "F053"  --信用证垫款
		WHEN BD.BUSINESSTYPE="1130040" THEN "F052"  --买方保理担保垫款
		WHEN BD.BUSINESSTYPE="1130070" THEN "F059"  --其他垫款
		WHEN SUBSTR(BD.BUSINESSTYPE,0,4)="1130" THEN "F059"
        ELSE 
        	CASE 
        	WHEN BC.PERMANENTASSETS='1' THEN 'F023'--固定贷款
        	ELSE  'F022'--经营贷款
        	end
        END) AS PRODUCT_TYPE_JS,
		BD.LCSTATUS AS LCSTATUS, --信用证状态 20241113
		BC.SERIALNO AS ACCT_NUM_CQCS --业务合同号客户风险 by:yxy 20241120
   FROM OMI.CL_BUSINESS_DUEBILL_HS BD --业务员借据信息
	LEFT JOIN OMI.CL_ADVANCED_INFO_HS AI
    ON AI.DUEBILLNO=BD.SERIALNO
	   AND AI.BEGNDT <= D_DATADATE 
	   AND AI.OVERDT > D_DATADATE 
	   AND AI.PARTID = V_PARTID     --取原账号用 20230802 lixiang更改
	left join omi.cl_business_duebill_hs BDOLD
    on BDOLD.Serialno = AI.OldDuebillNO
   and BDOLD.begndt <= D_DATADATE
   AND BDOLD.overdt > D_DATADATE
   AND BDOLD.partid = V_PARTID  --by qzj 20231204 同生产east逻辑，目的是取结局贷款用途字段	
  left join omi.cl_business_contract_hs BCOLD
    on BCOLD.Serialno = BDOLD.RelativeSerialNo2
   and BCOLD.begndt <= D_DATADATE
   AND BCOLD.overdt > D_DATADATE
   AND BCOLD.partid = V_PARTID  --by qzj 20231204 同生产east逻辑，目的是取结局贷款用途字段	      
	LEFT JOIN OMI.BS_ACCEPT_DETAILS_VIEW_HS ADV  --取原账号用 20230911 lixiang添加
	ON AI.OLDDUEBILLNO=ADV.warrant_no
    AND ADV.BEGNDT <= D_DATADATE 
	   AND ADV.OVERDT > D_DATADATE 
	   AND ADV.PARTID = V_PARTID
	LEFT JOIN OMI.CL_BUSINESS_CONTRACT_HS BC --业务合同信息
           ON BD.RELATIVESERIALNO2=BC.SERIALNO
          AND BC.BEGNDT <= D_DATADATE 
          AND BC.OVERDT > D_DATADATE 
          AND BC.PARTID = V_PARTID
    LEFT JOIN OMI.CL_CUSTOMER_INFO_HS CI --客户基本信息
           ON CI.CUSTOMERID=BD.CUSTOMERID 
          AND CI.BEGNDT <= D_DATADATE 
          AND CI.OVERDT > D_DATADATE 
          AND CI.PARTID = V_PARTID
    LEFT JOIN SUBQUERY_1 CODE --子查询
           ON CODE.ITEMNO=BD.BUSINESSCURRENCY
    LEFT JOIN OMI.CL_BUSINESS_TYPE_HS BT --业务品种设置表
           ON BD.BUSINESSTYPE=BT.TYPENO 
          AND BT.BEGNDT <= D_DATADATE 
          AND BT.OVERDT > D_DATADATE 
          AND BT.PARTID = V_PARTID
    LEFT JOIN OMI.CL_ENT_INFO_HS T7 --对公客户信息表
           ON BD.CUSTOMERID=T7.CUSTOMERID 
          AND T7.BEGNDT <= D_DATADATE 
          AND T7.OVERDT > D_DATADATE 
          AND T7.PARTID = V_PARTID
    LEFT JOIN OMI.CL_BUSINESS_PUTOUT_HS BP --业务出账信息
           ON BP.SERIALNO=BD.RELATIVESERIALNO1 
          AND BP.BEGNDT <= D_DATADATE 
          AND BP.OVERDT > D_DATADATE 
          AND BP.PARTID = V_PARTID
          --按照金数逻辑取值
/*    LEFT JOIN (SELECT CR.SERIALNO,MAX(G.GUARANTYTYPE) AS GUARANTYTYPE ,COUNT(G.GUARANTYTYPE) AS GUARANTYTYPE_NUMS ,MAX(GI.GUARANTYTYPE) AS GUARANTYTYPE_GI
	            FROM OMI.CL_GUARANTY_CONTRACT_HS G
	            INNER JOIN OMI.CL_CONTRACT_RELATIVE_HS CR --合同关联表
                       ON CR.OBJECTNO = G.SERIALNO
                      AND CR.BEGNDT <= D_DATADATE
                      AND CR.OVERDT > D_DATADATE
                      AND CR.PARTID = V_PARTID
	            LEFT JOIN OMI.CL_GUARANTY_RELATIVE_HS GR --担保合同关系
			           ON G.SERIALNO = GR.CONTRACTNO
			          AND GR.BEGNDT <= D_DATADATE
			          AND GR.OVERDT > D_DATADATE
			          AND GR.PARTID = V_PARTID
			    LEFT JOIN OMI.CL_GUARANTY_INFO_HS GI  -- 担保信息表
			           ON GR.GUARANTYID = GI.GUARANTYID
			          AND GI.BEGNDT <= D_DATADATE
			          AND GI.OVERDT > D_DATADATE
			          AND GI.PARTID = V_PARTID
				WHERE G.BEGNDT <= D_DATADATE AND G.OVERDT > D_DATADATE AND G.PARTID = V_PARTID
				GROUP BY CR.SERIALNO) GC --担保合同信息表*/
          LEFT JOIN (select BC.SerialNo ,max(BC.VouchType) as BCVouchType,max(GC.Vouchtype) as VouchType
		,count(distinct substr(GC.GUARANTYTYPE,0,3))  AS dblxs
		,max(substr(GC.GUARANTYTYPE,0,3))  AS dbfs
		,count(case when GC.Vouchtype like "02010%" then GC.SerialNo end) as fdcdys
		,count(case when GC.Vouchtype not like "005%" then GC.SerialNo end) as qtdbs
		from omi.cl_BUSINESS_CONTRACT_hs BC
		left join omi.cl_CONTRACT_RELATIVE_hs CR on CR.SerialNo=BC.SerialNo
			AND CR.begndt<=D_DATADATE
			AND CR.overdt> D_DATADATE
			AND CR.partid= V_PARTID
		left join omi.cl_GUARANTY_CONTRACT_hs  GC on CR.ObjectNo = GC.SerialNo   and GC.ContractStatus in ("020","040")
			AND GC.begndt<= D_DATADATE
			AND GC.overdt> D_DATADATE
			AND GC.partid= V_PARTID
		where 
			bc.begndt<= D_DATADATE
			AND bc.overdt> D_DATADATE
			AND bc.partid= V_PARTID
	--	and BC.Balance>0 
	--	and GC.ContractStatus in ("020","040")  
		--and (BC.BusinessType like "2%" /*or BC.BUSINESSTYPE="2070"*/)
		group by  BC.SerialNo
	)  GC
           ON GC.SERIALNO=BC.SERIALNO  
    LEFT JOIN OMI.KN_KLNB_DKHKSX_HS HK --贷款账户还款表
           ON BD.SERIALNO=HK.DKJIEJUH 
          AND HK.BEGNDT <= D_DATADATE 
          AND HK.OVERDT > D_DATADATE 
          AND HK.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KLNA_DKZHZB_HS KD --贷款账户主表
           ON BD.SERIALNO=KD.DKJIEJUH 
          AND KD.BEGNDT <= D_DATADATE 
          AND KD.OVERDT > D_DATADATE 
          AND KD.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KFAP_YEWUPZ_HS KU 
           ON KU.YEWUSX01=KD.CHANPDMA 
          AND KU.BEGNDT <= D_DATADATE
           AND KU.OVERDT > D_DATADATE 
           AND KU.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KFAP_YESHUX_HS KY 
           ON KY.YEWUBIMA=KU.YEWUBIMA 
          AND KY.BEGNDT <= D_DATADATE 
          AND KY.OVERDT > D_DATADATE 
          AND KY.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KLNB_DKJXSX_HS TT 
           ON KD.DKJIEJUH=TT.DKJIEJUH 
          AND TT.BEGNDT <= D_DATADATE 
          AND TT.OVERDT > D_DATADATE 
          AND TT.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KLNB_DKFKSX_HS KF
           ON BD.SERIALNO=KF.DKJIEJUH 
          AND KF.BEGNDT <= D_DATADATE 
          AND KF.OVERDT > D_DATADATE 
          AND KF.PARTID = V_PARTID
    LEFT JOIN omi.kn_ktaa_nbfhzh_hs hb
           ON hb.zhanghao = kf.dkrzhzhh
          AND hb.begndt <= D_DATADATE
          AND hb.overdt > D_DATADATE
          AND hb.partid = V_PARTID
	LEFT JOIN L_PUBL_ORG_BRA LJG 
		  ON hb.zhyngyjg=LJG.ORG_NUM
		  AND LJG.DATA_DATE=TO_CHAR(D_DATADATE,'YYYYMMDD') 
    LEFT JOIN  omi.kn_klnb_dkstzf_hs kkd       
           ON kf.dkjiejuh = kkd.dkjiejuh and kkd.stzfclzt = '6' 
          AND kkd.begndt<=D_DATADATE
          AND kkd.overdt>D_DATADATE
          AND kkd.partid=V_PARTID
    
    LEFT JOIN OMI.IS_BPD_HS BPD --进口信用证下单据业务信息
           ON BPD.JJH=BD.SERIALNO 
          AND BPD.BEGNDT <= D_DATADATE 
          AND BPD.OVERDT > D_DATADATE 
          AND BPD.PARTID = V_PARTID
    LEFT JOIN OMI.IS_TRN_HS TRN --国结系统汇率表
           ON TRN.OBJINR=BPD.INR 
          AND TRN.OBJTYP='BPD' 
          AND TRN.INIFRM='BPTFFT' 
          AND TRN.RELFLG IN ('R','F') 
          AND TRN.BEGNDT <= D_DATADATE 
          AND TRN.OVERDT > D_DATADATE 
          AND TRN.PARTID = V_PARTID
    LEFT JOIN OMI.IS_GLE_HS GLE2 --当事人联系人表
           ON GLE2.TRNINR=TRN.INR 
          AND GLE2.DBTCDT='C' 
         -- AND GLE2.DBTCDT='LO_CO1' 
		 AND GLE2.DBTDFT = 'LO-CO1' --by qzj 20231121 同老east逻辑，否则部分贷款入账账户名称取不到值
          AND GLE2.BEGNDT <= D_DATADATE 
          AND GLE2.OVERDT > D_DATADATE 
          AND GLE2.PARTID = V_PARTID
    
	LEFT JOIN (SELECT KDZ.DKJIEJUH,MIN(KDZ.DAOQRIQI) AS DAOQRIQI FROM OMI.KN_KLNB_DZQGJH_HS KDZ    
         WHERE  KDZ.BEGNDT <= D_DATADATE
          AND KDZ.OVERDT > D_DATADATE
          AND KDZ.PARTID = V_PARTID
          AND KDZ.DAOQRIQI > TO_CHAR(D_DATADATE,'YYYYMMDD')
          GROUP BY KDZ.DKJIEJUH) KDZ
          ON KD.DKJIEJUH=KDZ.DKJIEJUH
    /*20230627.kongyongjie.此子查询未用到且逻辑待确认，先删除
	LEFT JOIN SUBQUERY_2 KDK --子查询
           ON KDK.DKJIEJUH=BD.SERIALNO*/
    LEFT JOIN OMI.CL_USER_INFO_HS UI --人员信息表
           ON BC.MANAGEUSERID=UI.USERID 
          AND UI.BEGNDT <= D_DATADATE 
          AND UI.OVERDT > D_DATADATE 
          AND UI.PARTID = V_PARTID
    LEFT JOIN OMI.HR_BD_PSNDOC_HS BPH --人员基本信息
           ON UI.CERTID=BPH.ID 
          AND BPH.BEGNDT <= D_DATADATE 
          AND BPH.OVERDT > D_DATADATE
          AND BPH.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KDPA_KEHUZH_HS KH 
           ON KF.DKRZHZHH=KH.KEHUZHAO 
          AND KH.BEGNDT <= D_DATADATE 
          AND KH.OVERDT > D_DATADATE 
          AND KH.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KTAA_NBFHZH_HS H 
           -- ON H.ZHANGHAO=KF.DKJIEJUH 
            ON H.ZHANGHAO=KF.dkrzhzhh --by qzj 20231121同老east
          AND H.BEGNDT <= D_DATADATE 
          AND H.OVERDT > D_DATADATE 
          AND H.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KDPA_KEHUZH_HS KK 
           ON GLE2.ACT=KK.KEHUZHAO 
          AND KK.BEGNDT <= D_DATADATE 
          AND KK.OVERDT > D_DATADATE 
          AND KK.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KLNB_ZCZRMX_HS KZ 
           ON BD.SERIALNO=KZ.DKJIEJUH 
          AND KZ.BEGNDT <= D_DATADATE 
          AND KZ.OVERDT > D_DATADATE 
          AND KZ.PARTID = V_PARTID
    LEFT JOIN SUBQUERY_3 LJQS --子查询
         ON LJQS.DKJIEJUH=BD.SERIALNO
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ --机构参数表
           -- ON NVL(KH.KAIHJIGO,H.ZHYNGYJG)=KJ.JIGOUHAO 
           ON kh.kaihjigo = kj.jigouhao 
          AND KJ.BEGNDT <= D_DATADATE 
          AND KJ.OVERDT > D_DATADATE 
          AND KJ.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ4 --机构参数表
           ON CASE WHEN BD.MFORGID='0090102' THEN '0090101' ELSE BD.MFORGID END=KJ4.JIGOUHAO 
          AND KJ4.BEGNDT <= D_DATADATE 
          AND KJ4.OVERDT > D_DATADATE 
          AND KJ4.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KDPA_KEHUZH_HS KH2
           ON HK.HUANKZHH=KH2.KEHUZHAO 
          AND KH2.BEGNDT <= D_DATADATE 
          AND KH2.OVERDT > D_DATADATE 
          AND KH2.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ2 --机构参数表
           ON KH2.KAIHJIGO=KJ2.JIGOUHAO 
          AND KJ2.BEGNDT <= D_DATADATE 
          AND KJ2.OVERDT > D_DATADATE 
          AND KJ2.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KDPA_KEHUZH_HS KH3 --内部分户账
           ON HK.HUANKZHH=KH3.KEHUZHAO 
          AND KH3.BEGNDT <= D_DATADATE 
          AND KH3.OVERDT > D_DATADATE 
          AND KH3.PARTID = V_PARTID
  LEFT JOIN omi.kn_ktaa_nbfhzh_hs KH4  --QZJ 20240202 解决还款账号所属行名称取不到值的问题
    ON HK.huankzhh = KH4.zhanghao
   AND KH4.begndt <= D_DATADATE
   AND KH4.overdt > D_DATADATE
   AND KH4.partid = v_partid		 
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ3 --机构参数表
           -- ON HK.HUANKZHH=KJ3.jigouhao 
            ON KH4.zhyngyjg=KJ3.jigouhao 
          AND KJ3.BEGNDT <= D_DATADATE 
          AND KJ3.OVERDT > D_DATADATE 
          AND KJ3.PARTID = V_PARTID
    LEFT JOIN SUBQUERY_4 B2 --子查询
           ON B2.CUSTOMERID=BD.CUSTOMERID
    LEFT JOIN OMI.CL_ARGI_CONTRACT_HS AG --
           ON AG.SERIALNO=BD.RELATIVESERIALNO2 
          AND AG.BEGNDT <= D_DATADATE 
          AND AG.OVERDT > D_DATADATE 
          AND AG.PARTID = V_PARTID
    LEFT JOIN OMI.CL_CUSTOMER_SPECIAL_HS CSP --
           ON BD.RELATIVESERIALNO2=CSP.SERIALNO 
          AND CSP.BEGNDT <= D_DATADATE 
          AND CSP.OVERDT > D_DATADATE 
          AND CSP.PARTID = V_PARTID
    LEFT JOIN OMI.I9_ECL_FINAL_RESULT_HS I9 --
           ON I9.ASSET_NO=BD.SERIALNO 
          AND I9.BEGNDT <= D_DATADATE 
          AND I9.OVERDT > D_DATADATE 
          AND I9.PARTID = V_PARTID
    LEFT JOIN M_DICT_REMAPPING_DL M --码值映射表
           ON M.BUSINESS_TYPE = 'RSUM' 
          AND M.DICT_CODE = 'CL-GREENLOANPURPOSE-GREEN_LOAN_TYPE' 
          AND M.ORI_VALUES = BC.GREENLOANPURPOSE
    LEFT JOIN M_DICT_REMAPPING_DL M1 --码值映射表
           ON M1.BUSINESS_TYPE = 'RSUM' 
          AND M1.DICT_CODE = 'CL-TYPENAME-ACCT_TYP-2' 
          AND M1.ORI_VALUES = BT.TYPENO
    LEFT JOIN SUBQUERY_5 KX --子查询
           ON KX.DKJIEJUH=BD.SERIALNO
	LEFT JOIN urdm.L_ACCT_LOAN L --贷款借据信息表(上期)
	       ON BD.SERIALNO = L.LOAN_NUM
		  AND L.DATE_SOURCESD = 'CL_1'
		  AND L.DATA_DATE = TO_CHAR(ADD_MONTHS(D_DATADATE,-1), 'YYYYMMDD')  -- 取上期
    LEFT JOIN NBFHZ NBZ --子查询
           ON CASE WHEN BT.TYPENAME LIKE '%福费廷%' THEN NVL(GLE2.ACT,(CASE WHEN
                HB.ZHANGHAO IS NULL THEN KF.DKRZHZHH
               WHEN KKD.DFZHANGH IS NOT NULL THEN KKD.DFZHANGH
               ELSE
               KF.DKRZHZHH END)) 
               ELSE 
          NVL((CASE WHEN HB.ZHANGHAO IS NULL THEN KF.DKRZHZHH
                    WHEN KKD.DFZHANGH IS NOT NULL THEN KKD.DFZHANGH
                    ELSE KF.DKRZHZHH
          END), GLE2.ACT) END = NBZ.ZHANGHAO
    LEFT JOIN NBFHZ_1 NBZ1 --子查询
           ON NVL(HK.HUANKZHH,GLE2.ACT)	= NBZ1.ZHANGHAO	
    LEFT JOIN (SELECT T.BEIZHUUU ,T.DKJIEJUH,T.XSHJUZHI,T.JIAOYIRQ,T.ZHHUJIZD,T.XUHAOOOO,  
			          ROW_NUMBER() OVER(PARTITION BY T.DKJIEJUH,T.XSHJUZHI ORDER BY T.XUHAOOOO DESC) AS RN 
	             FROM OMI.KN_KLNL_DKBGMX_HS T   
 		        WHERE T.JIAOYIRQ <=TO_CHAR(D_DATADATE,'YYYYMMDD') 
 		          AND T.ZHHUJIZD = 'huankzhh') HXBZ
 		           ON HXBZ.DKJIEJUH =BD.SERIALNO
 		          AND HXBZ.XSHJUZHI=NVL(HK.HUANKZHH,GLE2.ACT)	
 		          AND HXBZ.RN = 1
	-- 20230630.kongyongjie.add-加工还款账号所属行字段
	LEFT JOIN (SELECT jjh FROM omi.is_lid_hs a WHERE A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE 
	           AND A.PARTID = V_PARTID AND A.DFLG!='D'
			   UNION 
			   SELECT jjh FROM omi.is_gid_hs b WHERE B.BEGNDT <= D_DATADATE AND B.OVERDT > D_DATADATE 
			   AND B.PARTID = V_PARTID) P
		   ON TRIM(BD.SERIALNO) = TRIM(P.JJH)
    LEFT JOIN M_DICT_REMAPPING_DL M2 --
	ON M2.ORI_VALUES = BC.BAILCURRENCY AND M2.BUSINESS_TYPE = 'RSUM' AND M2.DICT_CODE = 'A0001'
	LEFT JOIN L_PUBL_ORG_BRA JG2
	ON kk.kaihjigo = JG2.ORG_NUM
	AND JG2.DATA_DATE =TO_CHAR(D_DATADATE,'YYYYMMDD')   -- wky 20240414 转为用8位日期
	LEFT JOIN ( SELECT relativeserialno,serialno FROM (
		SELECT  relativeserialno,serialno,occurdate,ROW_NUMBER() OVER (PARTITION BY relativeserialno ORDER BY serialno DESC ) AS rn  FROM  OMI.CL_BUSINESS_WASTEBOOK_HS  
				WHERE OCCURDATE >= to_char(TRUNC(D_DATADATE, 'MM'),'YYYYMMDD') 
				AND OCCURDATE <= to_char(TRUNC(D_DATADATE, 'DD'),'YYYYMMDD')
				AND  OCCURSUBJECT = '0'		      
		 ) WHERE rn=1) BW 
		ON  BD.SERIALNO =BW.RELATIVESERIALNO 
	LEFT JOIN  omi.kn_kfap_neibhs_hs  nbhs    --SPE20240104035  20240108修改  涉及法人透支相关科目号，需要解析  qzj 20240515 原因同item_cd取值 同生产east
		ON nbhs.nbuhesdm=substr(ky.yeshux09,4)
		AND nbhs.begndt<=D_DATADATE
		AND nbhs.overdt>D_DATADATE
		AND nbhs.partid=v_partid
    LEFT JOIN omi.kn_kfap_kjkemu_hs kjj --SPE-20240715-0065
          ON kjj.kemuhaoo = nvl(nbhs.kemuhaoo, ky.yeshux09)
            --ON kjj.kemuhaoo = ky.yeshux09
         AND kjj.begndt <= D_DATADATE
         AND kjj.overdt > D_DATADATE
         AND kjj.partid = v_partid		
		LEFT JOIN (select relativeserialno,min(lastmaturity) as maturity from omi.cl_business_extension_hs be
	WHERE be.BEGNDT <= D_DATADATE AND be.OVERDT > D_DATADATE AND be.PARTID = V_PARTID
	group by be.relativeserialno) be
	ON be.relativeserialno = BD.serialno --mod by zwk 取展期时原始到期日	
/*	LEFT JOIN OMI.CL_CUSTOMER_INFO_HS CUST_INFO	--by:yxy 20240809 逻辑来源于信贷,应业务核对数据要求,从信贷段分离普惠对公数据
	       ON BD.CUSTOMERID = CUST_INFO.CUSTOMERID
		  AND CUST_INFO.BEGNDT <= D_DATADATE
		  AND CUST_INFO.OVERDT > D_DATADATE
		  AND CUST_INFO.PARTID = V_PARTID
*/
	LEFT JOIN OMI.CL_ENT_INFO_HS EII --对公客户信息表	--BY:YXY 20241023 解决贷款投向行业为空问题
	       ON BD.CUSTOMERID = EII.CUSTOMERID 
		  AND EII.BEGNDT <= D_DATADATE AND EII.OVERDT > D_DATADATE AND EII.PARTID = V_PARTID
	LEFT JOIN (SELECT CI.*,ROW_NUMBER() OVER(PARTITION BY customername ORDER BY INPUTDATE DESC) RN
	                 FROM OMI.CL_CUSTOMER_INFO_HS CI
	                 WHERE CI.BEGNDT <= D_DATADATE AND CI.OVERDT > D_DATADATE AND CI.PARTID = V_PARTID ) CII --
		   ON CII.CUSTOMERID = EII.CUSTOMERID
		  AND CII.RN = 1 --BY:YXY 20241023 解决贷款投向行业为空问题
	LEFT JOIN OMI.KN_KCFB_CFDGJC_HS KC --对公客户基本信息表 --BY:YXY 20241023 解决贷款投向行业为空问题
		   -- ON KC.KEHUZHWM = CII.CUSTOMERNAME
		   ON KC.KEHUZHWM = CII.MFCUSTOMERID --20241101  通过核心客户号关联，通过名称关联会有重复数据
		  AND KC.BEGNDT <= D_DATADATE AND KC.OVERDT > D_DATADATE AND KC.PARTID = V_PARTID
    LEFT JOIN (SELECT XDKH.*,ROW_NUMBER() OVER(PARTITION BY XDKH.MFCUSTOMERID ORDER BY XDKH.BEGNDT DESC) RN 
                 FROM OMI.CL_CUSTOMER_XINDAIKEHU_HS XDKH
                WHERE XDKH.PARTID = V_PARTID) XDKH    --by 20241210 
    ON KC.KEHUHAOO = XDKH.MFCUSTOMERID AND XDKH.RN = 1
    WHERE ((BD.BUSINESSTYPE LIKE '1%' AND BD.BUSINESSTYPE NOT LIKE '1020%')  --'1020%'  票据融资
       OR BC.BUSINESSTYPE='2070')  --委托贷款
      AND BD.BEGNDT <= D_DATADATE AND BD.OVERDT > D_DATADATE AND BD.PARTID = V_PARTID;
      

    V_DATA_COUNT := SQL%ROWCOUNT;
    V_STEP_FLAG := 1;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);
    
    V_STEP_ID   := V_STEP_ID + 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '第2段个贷加工逻辑';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

	INSERT INTO L_ACCT_LOAN 
    (ACCT_NUM,  --合同号
     ACCT_STS,  --账户状态
     ACCU_INT_AMT,  --应计利息
     ACCU_INT_FLG,  --计息标志
     ACTUAL_MATURITY_DT,  --实际到期日期
     BASE_INT_RAT,  --基准利率
     BOOK_TYPE,  --账户种类
     CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
     CANCEL_FLG,  --核销标志
     CIRCLE_LOAN_FLG,  --循环贷款标志
     CONSECUTE_TERM_NUM,  --连续欠款期数
     CONSUME_LOAN_ADD_TYPE,  --消费贷款补充细类
     CUMULATE_TERM_NUM,  --累计欠款期数
     CURRENT_TERM_NUM,  --当前还款期数
     CURR_CD,  --币种
     CUST_ID,  --客户号
     DATA_DATE,  --数据日期
     DATE_SOURCESD,  --数据来源
     DEPARTMENTD,  --归属部门
     DRAWDOWN_AMT,  --放款金额
     DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
     DRAWDOWN_DT,  --放款日期
     DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
     DRAWDOWN_TYPE,  --放款方式
     EMP_ID,  --信贷员员工号
     ENTRUST_PAY_AMT,  --受托支付金额
     EXTENDTERM_FLG,  --展期标志
     FINISH_DT,  --结清日期
     FLOAT_TYPE,  --参考利率类型
     FUND_USE_LOC_CD,  --贷款资金使用位置
     GENERALIZE_LOAN_FLG,  --普惠型贷款标志
     GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
     GREEN_BALANCE,  --绿色信贷余额
     GREEN_LOAN_FLG,  --绿色贷款标志
     GREEN_LOAN_TYPE,  --绿色贷款用途分类
     GREEN_PURPOSE_CD,  --绿色融资投向
     INDEPENDENCE_PAY_AMT,  --自主支付金额
     INDUST_TRAN_FLG,  --工业企业技术改造升级标识
     INTERNET_LOAN_FLG,  --互联网贷款标志
     IN_DRAWDOWN_DT,  --转入贷款的原借据发放日期
     IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
     IS_REPAY_OPTIONS,  --是否内嵌提前还款权
     ITEM_CD,  --科目号
     I_OD_DT,  --利息逾期日期
     LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
     LOAN_ACCT_BAL,  --贷款余额
     LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
     LOAN_ACCT_NAME,  --贷款入账账户名称
     LOAN_ACCT_NUM,  --贷款入账账号
     LOAN_BUSINESS_TYP,  --贷款用途分类
     LOAN_BUY_INT,  --是否转入贷款
     LOAN_FHZ_NUM,  --贷款分户账账号
     LOAN_GRADE_CD,  --五级分类代码
     LOAN_NUM,  --贷款编号
     LOAN_NUM_OLD,  --原贷款编号
     LOAN_PURPOSE_AREA,  --贷款投向地区
     LOAN_PURPOSE_CD,  --贷款投向
     LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
     LOAN_SELL_INT,  --转出标志
     LOW_RISK_FLAG,  --是否低风险业务
     MAIN_GUARANTY_TYP,  --主要担保方式
     MATURITY_DT,  --原始到期日期
     MON_INT_INCOM,  --本月利息收入
     NEXT_INT_PAY_DT,  --下一付息日
     NEXT_REPRICING_DT,  --下一利率重定价日
     OD_DAYS,  --逾期天数
     OD_FLG,  --逾期标志
     OD_INT,  --逾期利息
     OD_INT_OBS,  --表外欠息
     OD_LOAN_ACCT_BAL,  --逾期贷款余额
     ORG_NUM,  --机构号
     ORIG_TERM,  --原始期限
     ORIG_TERM_TYPE,  --原始期限类型
     PAY_ACCT_BANK,  --还款账号所属行名称
     PAY_ACCT_NUM,  --还款账号
     PRICING_BASE_TYPE,  --定价基础类型
     P_OD_DT,  --本金逾期日期
     REAL_INT_RAT,  --实际利率
     RENEW_FLG,  --无还本续贷标志
     REPAY_FLG,  --借新还旧标志
     REPAY_TERM_NUM,  --还款总期数
     RESCHED_FLG,  --重组标志
     TAX_RELATED_FLG,  --银税合作贷款标志
     TRANSFERS_LOAN_FLG,  --转贷款标志
     USEOFUNDS,  --贷款用途
     ACCT_TYP,  --账户类型
     ACCT_TYP_DESC,  --账户类型说明
     REPAY_TYP,  --还款方式_2
     REPAY_TYP_DESC,  --还款方式说明
     INT_RATE_TYP,  --利率类型
     INT_REPAY_FREQ,  --利息还款频率
     INT_REPAY_FREQ_DESC,  --利息还款频率说明
     INDUST_STG_TYPE,  --战略新兴产业类型
     GUARANTY_TYP,  --贷款担保方式
     INT_ADJEST_AMT,  --利息调整
	 --CSRQ,  --出生日期折年龄
	 ACCT_TYP_JS,	--账户类型_金数
	 guaranty_typ_js, --贷款担保方式(金数)
	 OD_TYPE_js,  --逾期类型_金数
	 LOAN_STATUS_js, --贷款状态_金数
	 loan_type_js --贷款类型_金数
    )
	  WITH SUBQUERY_1 AS 
            (SELECT DISTINCT GT.GL_AC_NO,GT.LOAN_NO 
             FROM OMI.PL_GL_TX_HS GT
            WHERE /*GT.BEGNDT <= D_DATADATE AND GT.OVERDT > D_DATADATE AND GT.PARTID = V_PARTID  --20231218 修改取消拉链日期
			  AND*/ GT.FUNC_ID='LNLNACTF' 
              AND GT.SEQ_NO='3'),
         SUBQUERY_10 AS 
            (SELECT A.LOAN_NO AS LOAN_NO,
                    MIN(PS.DUE_DT) AS DUE_DT
            FROM OMI.PL_LOAN_HS A
            LEFT JOIN OMI.PL_PAYM_SCHED_HS PS 
                   ON A.LOAN_NO=PS.LOAN_LOAN_NO
                  AND PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID
		    LEFT JOIN OMI.PL_od_int_log_HS oi
                   ON PS.LOAN_LOAN_NO = OI.LOAN_NO
                  AND PS.PERD_NO = OI.PERD_NO
                  AND OI.BEGNDT <=D_DATADATE AND OI.OVERDT > D_DATADATE AND OI.PARTID = V_PARTID
            WHERE PS.DUE_DT <= TO_CHAR(D_DATADATE,'YYYYMMDD')
		      AND (NVL(PS.INT_AMT,0) - NVL(PS.SETL_INT,0) > 0 OR oi.int_amt-oi.setl_int-oi.wv_int > 0)
              AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
            GROUP BY A.LOAN_NO),
         SUBQUERY_11 AS 
            (SELECT A.LOAN_NO AS LOAN_NO,
                    PS.INT_RATE AS INT_RATE
            FROM OMI.PL_LOAN_HS A
            LEFT JOIN  OMI.PL_PAYM_SCHED_HS PS 
                   ON A.LOAN_NO=PS.LOAN_LOAN_NO
                  AND PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID
            WHERE PS.PERD_NO=0
              AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID),
         SUBQUERY_12 AS 
            (SELECT A.LOAN_NO AS LOAN_NO,
                    MAX(SL.VAL_DT) AS VAL_DT
            FROM OMI.PL_LOAN_HS A
            LEFT JOIN OMI.PL_SETLMT_LOG_HS SL 
                   ON A.LOAN_NO=SL.LOAN_NO
                  AND SL.BEGNDT <= D_DATADATE AND SL.OVERDT > D_DATADATE AND SL.PARTID = V_PARTID
                WHERE A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
            GROUP BY A.LOAN_NO),
         SUBQUERY_2 AS 
            (SELECT DISTINCT L1.LS_APPL_DRAWDN_SEQ,L1.APPL_DRAWDN_DISB_REF,L1.APPL_DRAWDN_DISB_NAME,L1.APPL_DRAWDN_DISB_AC_BCH
            FROM OMI.PL_LS_APPL_DRAWDN_DISB_HS L1
           WHERE L1.BEGNDT <= D_DATADATE AND L1.OVERDT > D_DATADATE AND L1.PARTID = V_PARTID),
         SUBQUERY_3 AS 
            (SELECT DISTINCT Z.LOAN_LOAN_NO, Z.ACCT_NO, Z.BKBH_BANK_BCH_CDE, Z.NAME 
               FROM OMI.PL_ATPY_INSTR_HS Z
              WHERE Z.BEGNDT <= D_DATADATE AND Z.OVERDT > D_DATADATE AND Z.PARTID = V_PARTID),
         SUBQUERY_4 AS 
            (SELECT R1.LS_APPL_SEQ, MAX(R1.LS_APPL_DRAWDN_SEQ) AS  LS_APPL_DRAWDN_SEQ 
            FROM OMI.PL_LS_APPL_DRAWDN_HS R1
           WHERE R1.BEGNDT <= D_DATADATE AND R1.OVERDT > D_DATADATE AND R1.PARTID = V_PARTID
            GROUP BY R1.LS_APPL_SEQ),
         SUBQUERY_5 AS 
            (SELECT GL.GL_AC_NO,KM.KEMUMNCH,GA.LNPA_LOAN_TYP
            FROM OMI.PL_GL_ASSGN_HS GA
            LEFT JOIN OMI.PL_G_L_ACCT_NO_HS GL
              ON GA.AI_RECVL_GL = GL.LDGR_CDE
             AND GL.BEGNDT <= D_DATADATE AND GL.OVERDT > D_DATADATE AND GL.PARTID = V_PARTID
            LEFT JOIN OMI.KN_KFAP_KJKEMU_HS KM
              ON GL.GL_AC_NO=KM.KEMUHAOO
             AND KM.BEGNDT <= D_DATADATE AND KM.OVERDT > D_DATADATE AND KM.PARTID = V_PARTID
           WHERE GA.BEGNDT <= D_DATADATE AND GA.OVERDT > D_DATADATE AND GA.PARTID = V_PARTID
            ),
         SUBQUERY_6 AS 
            (SELECT PS.LOAN_LOAN_NO,MIN(PS.PERD_NO) AS DQQS
            FROM  OMI.PL_LOAN_HS A
            INNER JOIN OMI.PL_PAYM_SCHED_HS PS
                    ON A.LOAN_NO=PS.LOAN_LOAN_NO
                   AND PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID
                 WHERE A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
                   AND PS.DUE_DT > TO_CHAR(D_DATADATE,'YYYYMMDD') 
            GROUP BY PS.LOAN_LOAN_NO
            ),
         SUBQUERY_7 AS 
            (SELECT COUNT(PS.LOAN_LOAN_NO) AS COUNT ,PS.LOAN_LOAN_NO AS LOAN_LOAN_NO
            FROM  OMI.PL_LOAN_HS A
            INNER JOIN OMI.PL_PAYM_SCHED_HS PS
               ON A.LOAN_NO=PS.LOAN_LOAN_NO
              AND PS.BEGNDT <=D_DATADATE
		      AND PS.OVERDT > D_DATADATE
		      AND PS.PARTID = v_partid
	        WHERE A.BEGNDT <=D_DATADATE
		      AND A.OVERDT > D_DATADATE
		      AND A.PARTID = v_partid
		      AND PS.DUE_DT <= TO_CHAR(D_DATADATE,'YYYYMMDD') 
		      AND PS.DUE_DT >= A.NEXT_DUE_DT
            GROUP BY PS.LOAN_LOAN_NO
            ),
         SUBQUERY_8 AS 
            (SELECT COUNT(PS.LOAN_LOAN_NO) AS COUNT ,PS.LOAN_LOAN_NO AS LOAN_LOAN_NO
            FROM  OMI.PL_LOAN_HS A
            INNER JOIN OMI.PL_PAYM_SCHED_HS PS
                    ON A.LOAN_NO=PS.LOAN_LOAN_NO
                   AND PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID
            LEFT JOIN (SELECT OIL.LOAN_TYP, OIL.LOAN_NO, OIL.PERD_NO, SUM(OIL.INT_AMT) AS INT_AMT, SUM(OIL.SETL_INT) AS SETL_INT, SUM(NVL(OIL.WV_INT,0)) AS WV_INT
                        FROM OMI.PL_OD_INT_LOG_HS OIL
                       WHERE OIL.BEGNDT <= D_DATADATE AND OIL.OVERDT > D_DATADATE AND OIL.PARTID = V_PARTID
                        GROUP BY OIL.LOAN_TYP,OIL.LOAN_NO,OIL.PERD_NO)  OI 
                   ON OI.LOAN_TYP=PS.LOAN_LNPA_LOAN_TYP AND OI.LOAN_NO=PS.LOAN_LOAN_NO AND OI.PERD_NO=PS.PERD_NO
            WHERE PS.PERD_NO>0
              AND PS.DUE_DT <= TO_CHAR(D_DATADATE,'YYYYMMDD')
              AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
              AND((PS.INT_AMT-PS.SETL_INT)+(PS.PRCP_AMT-PS.SETL_PRCP)>0 OR  (OI.INT_AMT-OI.SETL_INT-NVL(OI.WV_INT,0))>0 OR PS.DUE_DT<NVL(PS.LAST_SETL_DT,TO_CHAR(D_DATADATE,'YYYYMMDD') ))
            GROUP BY PS.LOAN_LOAN_NO),
         SUBQUERY_9 AS 
            (SELECT A.LOAN_NO AS LOAN_NO,
                    MIN(PS.DUE_DT) AS DUE_DT
            FROM OMI.PL_LOAN_HS A
            LEFT JOIN  OMI.PL_PAYM_SCHED_HS PS 
                   ON A.LOAN_NO=PS.LOAN_LOAN_NO
                  AND PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID
            WHERE PS.PRCP_AMT-PS.SETL_PRCP>0
              AND PS.DUE_DT <= TO_CHAR(D_DATADATE,'YYYYMMDD')
              AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
            GROUP BY A.LOAN_NO),
          SUBQUERY_13 AS(   --逾期贷款余额
          SELECT A.LOAN_NO,SUM(CASE 
          WHEN A.NEXT_DUE_DT<=TO_CHAR(D_DATADATE,'YYYYMMDD')  THEN NVL(PS.PRCP_AMT,0) - NVL(PS.SETL_PRCP,0)
          ELSE 0 
          END) AS YQDKYE 
          FROM OMI.PL_LOAN_HS A --贷款主表
          LEFT JOIN  OMI.PL_PAYM_SCHED_HS PS --还款计划明细表
            ON A.LOAN_NO=PS.LOAN_LOAN_NO 
           AND PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID
         WHERE A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
           AND PS.DUE_DT <= TO_CHAR(D_DATADATE,'YYYYMMDD')
         GROUP BY A.LOAN_NO
          )
    SELECT A.LOAN_NO AS ACCT_NUM,  --合同号
        CASE WHEN A.STS='ACTV' AND A.NEXT_DUE_DT < TO_CHAR(D_DATADATE,'YYYYMMDD') THEN '2'
		     WHEN A.STS='ACTV'  THEN '1'
             WHEN A.STS='SETL' AND A.DEBT_STS='NORM'  THEN '3'     
        END AS ACCT_STS,  --账户状态
        NVL(PS.INT_AMT,0) AS ACCU_INT_AMT,  --应计利息
        'Y' AS ACCU_INT_FLG,  --计息标志
        TO_DATE(A.LAST_DUE_DT) AS ACTUAL_MATURITY_DT,  --实际到期日期
        REPLACE(A.BASE_RATE,'%','') AS BASE_INT_RAT,  --基准利率
        '2' AS BOOK_TYPE,  --账户种类
        'N' AS CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
        CASE WHEN A.STS='SETL' AND A.DEBT_STS='CHRGO' THEN 'Y' ELSE 'N' END AS CANCEL_FLG,  --核销标志
        NVL(WW.REVV_IND,'N') AS CIRCLE_LOAN_FLG,  --循环贷款标志
        NVL(TO_NUMBER(TMP3.COUNT),0) AS CONSECUTE_TERM_NUM,  --连续欠款期数
        CASE WHEN A.PRPS_FIN IN ('09','14') THEN 'A' ELSE '' END AS CONSUME_LOAN_ADD_TYPE,  --消费贷款补充细类
        CASE WHEN NVL(TO_NUMBER(TMP4.COUNT),0)<NVL(TO_NUMBER(TMP3.COUNT),0) THEN NVL(TO_NUMBER(TMP3.COUNT),0) ELSE NVL(TO_NUMBER(TMP4.COUNT),0) END AS CUMULATE_TERM_NUM,  --累计欠款期数
        NVL(TMP2.DQQS,0) AS CURRENT_TERM_NUM,  --当前还款期数
        'CNY' AS CURR_CD,  --币种
        CASE WHEN LENGTH(D.CUST_NO) = 1 THEN D.ID_NO ELSE D.CUST_NO END AS CUST_ID,  --客户号
        TO_CHAR(D_DATADATE,'YYYYMMDD') AS DATA_DATE,  --数据日期
        'PL_2' AS DATE_SOURCESD,  --数据来源
        'grdk' AS DEPARTMENTD,  --归属部门
        A.ORIG_PRCP AS DRAWDOWN_AMT,  --放款金额
        A.BASE_RATE+A.SPRD AS DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
        TO_DATE(A.ACTV_DT) AS DRAWDOWN_DT,  --放款日期
        S11.INT_RATE AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
        CASE WHEN F.APPL_PPSL_PAY_WAY='ST' THEN 'B'
          WHEN F.APPL_PPSL_PAY_WAY='ZZ' THEN 'A'
          WHEN A.LNPA_LOAN_TYP LIKE '135%' THEN 'A'
          ELSE 'B'
        END AS DRAWDOWN_TYPE,  --放款方式
        CASE WHEN LENGTH(A.AOFR_ACCT_OFFCR_CDE)=6 THEN A.AOFR_ACCT_OFFCR_CDE ELSE BP.CODE END AS EMP_ID,  --信贷员员工号
        CASE WHEN F.APPL_PPSL_PAY_WAY='ST' THEN A.ORIG_PRCP
          WHEN F.APPL_PPSL_PAY_WAY<>'ZZ' AND A.LNPA_LOAN_TYP NOT LIKE '135%' THEN A.ORIG_PRCP
          ELSE ''
        END AS ENTRUST_PAY_AMT,  --受托支付金额
        'N' AS EXTENDTERM_FLG,  --展期标志
        CASE  --WHEN A.LOAN_NO = '108150047570100' THEN '2018-06-28' --数据问题
              WHEN A.SALE_IND = 'Y' THEN TO_DATE(LOAN.SALE_DT)
              WHEN A.STS='SETL' AND A.DEBT_STS ='NORM' THEN TO_DATE(NVL(S12.VAL_DT,A.LAST_SETL_DT))
              WHEN A.STS='SETL' AND A.DEBT_STS ='CHRGO' THEN TO_DATE(GB.LAST_APPR_DT)
              ELSE '' 
        END AS FINISH_DT,  --结清日期
        CASE 
          WHEN A.CIS_STS='LPR' THEN 'A' 
          ELSE 'B'
        END AS FLOAT_TYPE,  --参考利率类型
        'I' AS FUND_USE_LOC_CD,  --贷款资金使用位置
        'N' AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
        'N' AS GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
        CASE 
          WHEN A.ENDOWMENT_INS_NO IS NOT NULL AND A.STS<>'SETL' AND A.DEBT_STS<>'CHRGO' THEN A.OS_PRCP
          ELSE ''
        END AS GREEN_BALANCE,  --绿色信贷余额
        NVL(A.RECOURSE,'N') AS GREEN_LOAN_FLG,  --绿色贷款标志
        M1.NEW_VALUES AS GREEN_LOAN_TYPE,  --绿色贷款用途分类
        M3.NEW_VALUES AS GREEN_PURPOSE_CD,  --绿色融资投向
        CASE WHEN F.APPL_PPSL_PAY_WAY='ZZ' OR A.LNPA_LOAN_TYP LIKE '135%' THEN A.ORIG_PRCP
          ELSE ''
        END AS INDEPENDENCE_PAY_AMT,  --自主支付金额
        CASE WHEN LALT1.APPL_LOAN_TARGET_FIELD='TU' THEN '1' 
          ELSE '2' 
        END AS INDUST_TRAN_FLG,  --工业企业技术改造升级标识
        'N' AS INTERNET_LOAN_FLG,  --互联网贷款标志
        '' AS IN_DRAWDOWN_DT,  --转入贷款的原借据发放日期
		'N' AS IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款  GJW待后续加工，统计客户在所有系统的贷款次数
        CASE 
          WHEN A.LNPA_LOAN_TYP LIKE '10%' AND ADD_MONTHS(A.ACTV_DT, 12) < D_DATADATE THEN 'N' 
          ELSE 'Y' 
        END AS IS_REPAY_OPTIONS,  --是否内嵌提前还款权
        GA.GL_AC_NO AS ITEM_CD,  --科目号
        TO_DATE(S10.DUE_DT) AS I_OD_DT,  --利息逾期日期
        CASE 
          WHEN LALT2.APPL_LOAN_TARGET_FIELD='TC' THEN 'Y' 
          ELSE 'N' 
        END AS LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
        CASE 
          WHEN A.STS='SETL' AND A.DEBT_STS='CHRGO' THEN 0 
          ELSE A.OS_PRCP 
        END AS LOAN_ACCT_BAL,  --贷款余额
        CASE 
          WHEN NVL(KJ.JIGOUZWM,KJ1.JIGOUZWM)='大连银行总行' THEN '大连银行股份有限公司' 
          ELSE NVL(KJ.JIGOUZWM,KJ1.JIGOUZWM) 
        END AS LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
        NVL(L.APPL_DRAWDN_DISB_NAME,Z.NAME) AS LOAN_ACCT_NAME,  --贷款入账账户名称
        NVL(NVL(L.APPL_DRAWDN_DISB_REF,GT.GL_AC_NO),Z.ACCT_NO) AS LOAN_ACCT_NUM,  --贷款入账账号
        '4' AS LOAN_BUSINESS_TYP,  --贷款用途分类
        'N' AS LOAN_BUY_INT,  --是否转入贷款
        A.LOAN_NO AS LOAN_FHZ_NUM,  --贷款分户账账号
        TRIM(CASE 
          WHEN A.LOAN_GRD IS NULL OR A.LOAN_GRD='100' THEN '1' 
          WHEN A.LOAN_GRD='200' THEN '2' 
          WHEN A.LOAN_GRD='300' THEN '3' 
          WHEN A.LOAN_GRD='400' THEN '4' 
          WHEN A.LOAN_GRD='500' THEN '5' 
        END) AS LOAN_GRADE_CD,  --五级分类代码
        E.LOAN_DRAWDN_NO AS LOAN_NUM,  --贷款编号
        A.REFINC_FURCHRG_NO AS LOAN_NUM_OLD,  --原贷款编号
        CASE 
          WHEN B.DIST_CDE ='460400' THEN '000000' 
          ELSE B.DIST_CDE 
        END AS LOAN_PURPOSE_AREA,  --贷款投向地区
        SUBSTR(LT.WEIGHT_CODE,5,1)||LT.CODE AS LOAN_PURPOSE_CD,  --贷款投向
        'CHN' AS LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
        A.SALE_IND AS LOAN_SELL_INT,  --转出标志
        'N' AS LOW_RISK_FLAG,  --是否低风险业务
        CASE 
          WHEN A.MAIN_GUR_TYP='BCOLL' THEN '0' 
          WHEN A.MAIN_GUR_TYP='COLL' THEN '1' 
          WHEN A.MAIN_GUR_TYP='GUAR' THEN '2' 
          WHEN A.MAIN_GUR_TYP='CREDIT' THEN '3' 
        END AS MAIN_GUARANTY_TYP,  --主要担保方式
        TO_DATE(A.LAST_DUE_DT) AS MATURITY_DT,  --原始到期日期
        '0' AS YEAR_INT_INCOM,  --本年利息收入
        CASE WHEN TO_DATE(A.NEXT_DUE_DT) > D_DATADATE THEN TO_DATE(A.NEXT_DUE_DT) END AS NEXT_INT_PAY_DT,  --下一付息日
  /*      CASE 
          WHEN A.NEXT_DUE_DT < TO_CHAR(D_DATADATE,'YYYYMMDD') THEN 
            CASE 
              WHEN A.RATE_MODE='FX' THEN TO_DATE(A.LAST_DUE_DT) 
              WHEN A.RATE_MODE='RV' THEN TO_DATE(A.NEXT_REPC_DT) 
            END 
        END AS NEXT_REPRICING_DT,  --下一利率重定价日*/
          CASE 
				WHEN A.RATE_MODE="FX" THEN TDH_TODATE(NVL(R1.LAST_DUE_DT,A.LAST_DUE_DT)) 
			ELSE 
			CASE 
				WHEN TDH_TODATE(NVL(R1.LAST_DUE_DT,A.LAST_DUE_DT))< TDH_TODATE(NVL(A.NEXT_REPC_DT,NVL(R1.LAST_DUE_DT,A.LAST_DUE_DT))) THEN TDH_TODATE(NVL(R1.LAST_DUE_DT,A.LAST_DUE_DT))
				ELSE NVL(TDH_TODATE(A.NEXT_REPC_DT),TDH_TODATE(NVL(R1.LAST_DUE_DT,A.LAST_DUE_DT)))
		END
		END AS NEXT_REPRICING_DT ,--下一利率重定价日-----金数新加
        DATEDIFF(D_DATADATE,TO_DATE(A.NEXT_DUE_DT)) AS OD_DAYS,  --逾期天数
        CASE 
          WHEN A.NEXT_DUE_DT<TO_CHAR(D_DATADATE,'YYYYMMDD') THEN 'Y' 
          ELSE 'N' 
        END AS OD_FLG,  --逾期标志
        NVL(LI.INT_RECVL_AMT,0) + NVL(LI.ODI_RECVL_AMT,0)+NVL(A.ADJ_BF_PRCP,0) AS OD_INT,  --逾期利息
        NVL(LI.INT_SUSP_RECVL_AMT,0)+NVL(LI.INT_NDV_RECVL_AMT,0)+NVL(LI.ODI_SUSP_RECVL_AMT,0)+NVL(LI.ODI_NDV_RECVL_AMT,0) AS OD_INT_OBS,  --表外欠息
        --S13.YQDKYE END AS OD_LOAN_ACCT_BAL,
        CASE WHEN A.SALE_IND = 'Y' THEN 0 ELSE S13.YQDKYE END AS OD_LOAN_ACCT_BAL,  --逾期贷款余额   by gjw 20231024
        B.COREBANK_BCH AS ORG_NUM,  --机构号
        A.LOAN_TNR AS ORIG_TERM,  --原始期限
        'M' AS ORIG_TERM_TYPE,  --原始期限类型
        CASE 
          WHEN NVL(KJ1.JIGOUZWM,KJ.JIGOUZWM)='大连银行总行' THEN '大连银行股份有限公司' 
          ELSE NVL(KJ1.JIGOUZWM,KJ.JIGOUZWM) 
        END AS PAY_ACCT_BANK,  --还款账号所属行名称
        Z.ACCT_NO AS PAY_ACCT_NUM,  --还款账号
        TRIM(CASE 
          WHEN A.CIS_STS='LPR' THEN 'C' 
          --修改取值金数基准利率类型同步老金数个人贷款表取值，兼容east个人信贷业借据表 利率类型取值规则  wzm 和ba张静祎确认后修改
          WHEN nvl(A.CIS_STS,'BIR')='BIR' THEN 'B02' 
          ELSE 'Z99' 
        END) AS PRICING_BASE_TYPE,  --定价基础类型
        TO_DATE(S9.DUE_DT) AS P_OD_DT,  --本金逾期日期
        (A.BASE_RATE+A.SPRD)*(1+A.INT_ADJ_PCT/100) AS REAL_INT_RAT,  --实际利率
        CASE 
          WHEN A.REFINC_FURCHRG_IND='E' THEN 'Y' 
          ELSE 'N' 
        END AS RENEW_FLG,  --无还本续贷标志
        CASE 
          WHEN A.REFINC_FURCHRG_IND='R' THEN 'Y' 
          ELSE 'N' 
        END AS REPAY_FLG,  --借新还旧标志
        A.TNR AS REPAY_TERM_NUM,  --还款总期数
        'N' AS RESCHED_FLG,  --重组标志
        'N' AS TAX_RELATED_FLG,  --银税合作贷款标志
        'N' AS TRANSFERS_LOAN_FLG,  --转贷款标志
        CASE 
          WHEN M.NEW_VALUES IS NOT NULL THEN M.NEW_VALUES 
          ELSE '其他贷款' 
        END AS USEOFUNDS,  --贷款用途
        TRIM(CASE WHEN A.PRPS_FIN = '07' THEN '0104'  --个人助学贷款   --add by gjw 20240110  为了核对G05修改
		    WHEN substr(a.lnpa_loan_typ,1,4) IN ('1010','1020') AND G.LOAN_GRP_PRI_DESC='个人住房贷款' THEN '010101'--住房按揭贷款   --add by gjw 20240110  为了核对G05修改
		    --WHEN substr(a.lnpa_loan_typ,1,4) IN ('1030','1040','1050','1060')  THEN '010201'--个人商业用房贷款   --add by gjw 20240329
			WHEN substr(a.lnpa_loan_typ,1,4) NOT IN ('1010','1020') AND G.LOAN_GRP_PRI_DESC='个人住房贷款' THEN '010199'--其他个人住房贷款   --add by gjw 20240110  为了核对G05修改
		    WHEN A.PRPS_FIN = '05' THEN '010301'--汽车贷款   --add by gjw 20240110  为了核对G05修改
			--WHEN G.LOAN_GRP_PRI_DESC='个人住房贷款'  THEN '010101'--住房按揭贷款
			--WHEN G.LOAN_GRP_PRI_DESC='个人汽车贷款'  THEN '010301'--汽车贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人质押贷款'  THEN '010399'--其他-个人质押贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人消费贷款'  THEN '010399'--消费贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人经营贷款'  THEN '010299'--个人经营性贷
			WHEN G.LOAN_GRP_PRI_DESC='个人授信额度'  THEN '010399'--其他-个人授信额度
			WHEN G.LOAN_GRP_PRI_DESC='个人政策性贷款' THEN '010399'--其他-政策性贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人委托贷款'  THEN '90'--委托贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人其他贷款'THEN '010399'--其他-个人其他贷款
			WHEN G.LOAN_GRP_PRI_DESC='联保体授信额度' THEN '010399'--其他-联保体授信额度
			ELSE '010399'--其他-个人其他贷款
			END) AS ACCT_TYP,  --账户类型
        TRIM(CASE WHEN G.LOAN_GRP_PRI_DESC='个人质押贷款' THEN '个人质押贷款'
          WHEN G.LOAN_GRP_PRI_DESC='个人授信额度' THEN '个人授信额度'
          WHEN G.LOAN_GRP_PRI_DESC='个人政策性贷款' THEN '政策性贷款'
          WHEN G.LOAN_GRP_PRI_DESC='个人其他贷款' THEN '个人其他贷款'
          WHEN G.LOAN_GRP_PRI_DESC='联保体授信额度' THEN '联保体授信额度'
		  WHEN substr(a.lnpa_loan_typ,1,4) NOT IN ('1010','1020') AND G.LOAN_GRP_PRI_DESC='个人住房贷款' THEN '其他个人住房贷款'  --add by gjw 20240110 
          ELSE ''
        END) AS ACCT_TYP_DESC,  --账户类型说明
        CASE --WHEN LP.INSTM_LN_IND='N' THEN '5'
		  WHEN LP.INSTM_LN_IND='N' AND ST.LOAN_NO IS NOT NULL THEN '5'
          WHEN LP.INSTM_LN_IND='Y'  THEN 
          CASE WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=1 THEN '1'
               WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=3 THEN '2'
               WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=6 THEN '3'
               WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=12 THEN '4' ELSE '7' END   --20230704
          ELSE '7'
        END AS REPAY_TYP,  --还款方式_2
        CASE WHEN A.INSTM_FREQ_UNIT_TYP = 'W' AND A.INSTM_FREQ_NUM_UNIT = 1 THEN '按周'
             WHEN A.INSTM_FREQ_UNIT_TYP = 'W' AND A.INSTM_FREQ_NUM_UNIT = 2 THEN '按双周'
		     WHEN LP.INSTM_LN_IND='Y'  AND A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT NOT IN ('1','3','6','12' ) THEN '还款'
        END AS REPAY_TYP_DESC,  --还款方式说明
        CASE WHEN A.RATE_MODE='FX' THEN 'F'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION IN ('NYF','DDA') THEN 'L5'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION ='FIX' AND A.NEXT_REPC_NUM='MONTH' THEN 'L2'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION ='FIX' AND A.NEXT_REPC_NUM='THREEMONTH' THEN 'L3'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION ='FIX' AND A.NEXT_REPC_NUM='SIXMONTH' THEN 'L4'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION ='FIX' AND A.NEXT_REPC_NUM='YEAR' THEN 'L5'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION ='NNR' AND A.INSTM_FREQ_UNIT_TYP='W' AND A.INSTM_FREQ_NUM_UNIT = 1 THEN 'L1'
          WHEN A.RATE_MODE='RV'  AND A.NEXT_REPC_OPTION ='NNR' AND A.INSTM_FREQ_UNIT_TYP='M' AND A.INSTM_FREQ_NUM_UNIT= 1 THEN 'L2'
          WHEN A.RATE_MODE='RV' AND A.NEXT_REPC_OPTION ='NNR' AND A.INSTM_FREQ_UNIT_TYP='M' AND A.INSTM_FREQ_NUM_UNIT= 3 THEN 'L3'
          WHEN A.RATE_MODE='RV' AND A.NEXT_REPC_OPTION ='NNR' AND A.INSTM_FREQ_UNIT_TYP='M' AND A.INSTM_FREQ_NUM_UNIT= 6 THEN 'L4'
          WHEN A.RATE_MODE='RV' AND A.NEXT_REPC_OPTION ='NNR' AND A.INSTM_FREQ_UNIT_TYP='M' AND A.INSTM_FREQ_NUM_UNIT= 12 THEN 'L5'
          WHEN A.RATE_MODE='RV' AND A.NEXT_REPC_OPTION ='OTH' THEN 'L9'
          WHEN A.RATE_MODE='RV' AND A.NEXT_REPC_OPTION ='IMM' THEN 'L9'
          ELSE 'L9'
        END AS INT_RATE_TYP,  --利率类型
        CASE --WHEN NVL(LP.IRREG_IND,'N')='N' THEN '07'
		  WHEN LP.INSTM_LN_IND='N' AND ST.LOAN_NO IS NULL THEN '07'
          WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=1 THEN '03'
          WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=3 THEN '04'
          WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=6 THEN '05'
          WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT=12 THEN '06'
          WHEN A.INSTM_FREQ_UNIT_TYP = 'W' AND A.INSTM_FREQ_NUM_UNIT=1 THEN '02'
          ELSE '99'
        END AS INT_REPAY_FREQ,  --利息还款频率
        CASE WHEN A.INSTM_FREQ_NUM_UNIT = 2 AND A.INSTM_FREQ_UNIT_TYP = 'W' THEN  '按双周结息'
          WHEN NVL(LP.IRREG_IND,'N')='N' THEN ''
          WHEN A.INSTM_FREQ_UNIT_TYP = 'M' AND A.INSTM_FREQ_NUM_UNIT IN ('1','3','6','12' )  THEN ''
          WHEN A.INSTM_FREQ_UNIT_TYP = 'W' AND A.INSTM_FREQ_NUM_UNIT =1 THEN ''
          ELSE '其他结息'
        END AS INT_REPAY_FREQ_DESC,  --利息还款频率说明
        CASE WHEN LALT3.APPL_LOAN_TARGET_FIELD='1' THEN '1' 
          WHEN LALT3.APPL_LOAN_TARGET_FIELD='2' THEN '2' 
          WHEN LALT3.APPL_LOAN_TARGET_FIELD='3' THEN '3' 
          WHEN LALT3.APPL_LOAN_TARGET_FIELD='4' THEN '4' 
          WHEN LALT3.APPL_LOAN_TARGET_FIELD='5' THEN '5' 
          WHEN LALT3.APPL_LOAN_TARGET_FIELD='6' THEN '6' 
          WHEN LALT3.APPL_LOAN_TARGET_FIELD='7' THEN '7' 
          ELSE 'N' 
        END AS INDUST_STG_TYPE,  --战略新兴产业类型
        C.L_TYPE AS GUARANTY_TYP,  --贷款担保方式
        0 AS INT_ADJEST_AMT,  --利息调整
		--TRUNC(MONTHS_BETWEEN(D_DATADATE,to_date(ii.DT_OF_BIRTH))/12)AS CSRQ  --出生日期折年龄
		--ii.DT_OF_BIRTH AS CSRQ,	--出生日期	--BY:YXY 20241121 去除该字段，规范出数逻辑
        TRIM(CASE WHEN A.PRPS_FIN = '07' THEN '0104'  --个人助学贷款   --add by gjw 20240110  为了核对G05修改
		    WHEN substr(a.lnpa_loan_typ,1,4) IN ('1010','1020') AND G.LOAN_GRP_PRI_DESC='个人住房贷款' THEN '010101'--住房按揭贷款
		    WHEN substr(a.lnpa_loan_typ,1,4) IN ('1030','1040','1050','1060')  THEN '010201'--个人商业用房贷款
			WHEN substr(a.lnpa_loan_typ,1,4) NOT IN ('1010','1020') AND G.LOAN_GRP_PRI_DESC='个人住房贷款' THEN '010199'--其他个人住房贷款
		    WHEN A.PRPS_FIN = '05' THEN '010301'--汽车贷款 
			--WHEN G.LOAN_GRP_PRI_DESC='个人住房贷款'  THEN '010101'--住房按揭贷款
			--WHEN G.LOAN_GRP_PRI_DESC='个人汽车贷款'  THEN '010301'--汽车贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人质押贷款'  THEN '010399'--其他-个人质押贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人消费贷款'  THEN '010399'--消费贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人经营贷款'  THEN '010299'--个人经营性贷
			WHEN G.LOAN_GRP_PRI_DESC='个人授信额度'  THEN '010399'--其他-个人授信额度
			WHEN G.LOAN_GRP_PRI_DESC='个人政策性贷款' THEN '010399'--其他-政策性贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人委托贷款'  THEN '90'--委托贷款
			WHEN G.LOAN_GRP_PRI_DESC='个人其他贷款'THEN '010399'--其他-个人其他贷款
			WHEN G.LOAN_GRP_PRI_DESC='联保体授信额度' THEN '010399'--其他-联保体授信额度
			ELSE '010399'--其他-个人其他贷款
			END) AS ACCT_TYP_JS,  --账户类型	区分金数和1104使用
		    CJS.L_TYPE   AS  guaranty_typ_js, --担保方式(金数)  
			case WHEN R1.LOAN_GRD= '100'  THEN ''
                 when nvl(r1.od_prcp_amt,0)>0 and (nvl(r1.od_int_amt,0)+nvl(r1.od_od_int_amt,0))>0 then '03'
                 else
                 case when  nvl(r1.od_prcp_amt,0)>0 then '01'
                      when (nvl(r1.od_int_amt,0)+nvl(r1.od_od_int_amt,0))>0 then '02'
                 else '' END 
				 END AS  OD_TYPE_js, --逾期类型（金数）
			CASE WHEN R1.LOAN_GRD= '100' THEN 
                         case when rll.cnt>0 then 'LS04' else  'LS01' end
                         ELSE 'LS03' END as LOAN_STATUS_js, --贷款状态_金数	
             R1.loan_grp2 AS loan_type_js	--贷款类型_金数  专项一批判断个体户小微企业主(四川商铺)	 
    FROM OMI.PL_LOAN_HS A --贷款主表
	/*INNER JOIN omi.pl_indiv_info_hs ii	--BY:YXY 20241121 去除该字段，规范出数逻辑
	        ON ii.ctif_id_type = A.ctif_id_type
		   AND ii.ctif_id_no=A.ctif_id_no 
		   AND ii.ctif_iss_ctry=A.ctif_iss_ctry
		   AND ii.begndt <= D_DATADATE AND ii.overdt > D_DATADATE AND ii.partid = v_partid  --BY CWX 20240111 参照生产逻辑，增加出生日期逻辑判断核对G05数据
	*/
	LEFT JOIN OMI.PL_ABS_LOAN_DETL_HS LOAN --入池资产主表
	       ON LOAN.LOAN_NO = A.LOAN_NO AND LOAN.ABS_SEQ = A.ABS_SEQ
	      AND LOAN.BEGNDT <= D_DATADATE AND LOAN.OVERDT > D_DATADATE AND LOAN.PARTID = V_PARTID
    LEFT JOIN OMI.PL_LOAN_ACTV_LOG_HS E --放款记录表
           ON E.LOAN_NO=A.LOAN_NO 
          AND E.BEGNDT <= D_DATADATE AND E.OVERDT > D_DATADATE AND E.PARTID = V_PARTID
    LEFT JOIN OMI.PL_LS_LOAN_GRP_HS G --贷款类型
           ON SUBSTR(A.LNPA_LOAN_TYP,1,2)=G.LOAN_GRP_CODE 
          AND G.BEGNDT <= D_DATADATE AND G.OVERDT > D_DATADATE AND G.PARTID = V_PARTID
    LEFT JOIN OMI.PL_CUST_INFO_HS D --客户信息表
           ON A.CTIF_ID_NO=D.ID_NO 
	      AND A.CTIF_ID_TYPE=D.ID_TYPE 
	      AND A.CTIF_ISS_CTRY=D.ISS_CTRY 
          AND D.BEGNDT <= D_DATADATE AND D.OVERDT > D_DATADATE AND D.PARTID = V_PARTID
    LEFT JOIN OMI.PL_BCH_DEPT_HS B --个贷机构表
           ON B.BCH_CDE=A.BHDT_BCH_CDE 
          AND B.BEGNDT <= D_DATADATE AND B.OVERDT > D_DATADATE AND B.PARTID = V_PARTID
    LEFT JOIN OMI.PL_GAST_BAD_DEBT_HS GB --
           ON A.LOAN_NO=GB.LOAN_NO 
          AND GB.BEGNDT <= D_DATADATE AND GB.OVERDT > D_DATADATE AND GB.PARTID = V_PARTID
    LEFT JOIN (SELECT PS.LOAN_LOAN_NO ,SUM(int_amt) int_amt
             FROM OMI.PL_PAYM_SCHED_HS PS --还款计划明细表           
            WHERE PS.BEGNDT <= D_DATADATE AND PS.OVERDT > D_DATADATE AND PS.PARTID = V_PARTID --int_amt
              AND due_dt > TO_CHAR(D_DATADATE,'YYYYMMDD')
            GROUP BY PS.LOAN_LOAN_NO ) PS
           ON A.LOAN_NO=PS.LOAN_LOAN_NO 
    LEFT JOIN OMI.PL_LOAN_INFO_HS LI --贷款信息表
           ON LI.LOAN_NO=A.LOAN_NO 
          AND LI.BEGNDT <= D_DATADATE AND LI.OVERDT > D_DATADATE AND LI.PARTID = V_PARTID
    LEFT JOIN OMI.PL_LOAN_PARA_HS LP --贷款品种
           ON A.LNPA_LOAN_TYP=LP.LOAN_TYP 
          AND LP.BEGNDT <= D_DATADATE AND LP.OVERDT > D_DATADATE AND LP.PARTID = V_PARTID
    /*LEFT JOIN OMI.PL_LS_APPL_LOAN_TARGET_HS LALT --贷前贷款投向领域表
           ON LALT.LS_APPL_SEQ=A.LS_APPL_SEQ 
          AND LALT.BEGNDT <= D_DATADATE AND LALT.OVERDT > D_DATADATE AND LALT.PARTID = V_PARTID*/
	LEFT JOIN (SELECT LS_APPL_SEQ,APPL_LOAN_TARGET_FIELD 
             FROM OMI.PL_LS_APPL_LOAN_TARGET_HS LALT --贷前贷款投向领域表  --存在同一申请书编号对应多投向领域，所以分开关联
            WHERE LALT.BEGNDT <= D_DATADATE AND LALT.OVERDT > D_DATADATE AND LALT.PARTID = V_PARTID
              AND APPL_LOAN_TARGET_FIELD = 'TU') LALT1
          ON LALT1.LS_APPL_SEQ=A.LS_APPL_SEQ 
    LEFT JOIN (SELECT LS_APPL_SEQ,APPL_LOAN_TARGET_FIELD 
             FROM OMI.PL_LS_APPL_LOAN_TARGET_HS LALT --贷前贷款投向领域表
            WHERE LALT.BEGNDT <= D_DATADATE AND LALT.OVERDT > D_DATADATE AND LALT.PARTID = V_PARTID
              AND APPL_LOAN_TARGET_FIELD = 'TC') LALT2
          ON LALT2.LS_APPL_SEQ=A.LS_APPL_SEQ 
    LEFT JOIN (SELECT LS_APPL_SEQ,APPL_LOAN_TARGET_FIELD 
             FROM OMI.PL_LS_APPL_LOAN_TARGET_HS LALT --贷前贷款投向领域表
            WHERE LALT.BEGNDT <= D_DATADATE AND LALT.OVERDT > D_DATADATE AND LALT.PARTID = V_PARTID
              AND APPL_LOAN_TARGET_FIELD = 'TC') LALT3
          ON LALT3.LS_APPL_SEQ=A.LS_APPL_SEQ
    LEFT JOIN OMI.PL_LS_APPL_PPSL_HS F --申请书信息表
           ON A.LS_APPL_SEQ=F.LS_APPL_SEQ 
          AND F.BEGNDT <= D_DATADATE AND F.OVERDT > D_DATADATE AND F.PARTID = V_PARTID
    LEFT JOIN OMI.PL_CR_LN_HDR_HS WW--信用额度信息主表
           ON A.CTIF_ID_TYPE = WW.CTIF_ID_TYPE
          AND A.CTIF_ID_NO = WW.CTIF_ID_NO
          AND A.CTIF_ISS_CTRY = WW.CTIF_ISS_CTRY
          AND A.CR_LINE = WW.CR_LINE
          AND WW.BEGNDT <= D_DATADATE AND WW.OVERDT > D_DATADATE AND WW.PARTID = V_PARTID
    LEFT JOIN OMI.PL_USR_PRFL_HS U --跟踪用户
           ON U.USR_ID=A.AOFR_ACCT_OFFCR_CDE 
          AND U.BEGNDT <= D_DATADATE AND U.OVERDT > D_DATADATE AND U.PARTID = V_PARTID
    LEFT JOIN OMI.HR_BD_PSNDOC_HS BP --人员基本信息
           ON BP.ID=U.CTIF_ID_NO 
          AND BP.BEGNDT <= D_DATADATE AND BP.OVERDT > D_DATADATE AND BP.PARTID = V_PARTID
    LEFT JOIN SUBQUERY_1 GT --子查询
           ON A.LOAN_NO=GT.LOAN_NO 
	LEFT JOIN SUBQUERY_4 R --子查询
           ON R.LS_APPL_SEQ=A.LS_APPL_SEQ
    LEFT JOIN SUBQUERY_2 L --子查询
           ON R.LS_APPL_DRAWDN_SEQ=L.LS_APPL_DRAWDN_SEQ
    LEFT JOIN SUBQUERY_3 Z --子查询
           ON Z.LOAN_LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_5 GA --
           ON A.LNPA_LOAN_TYP=GA.LNPA_LOAN_TYP
    LEFT JOIN OMI.PL_BCH_DEPT_HS LBD1 --个贷机构表
           ON Z.BKBH_BANK_BCH_CDE=LBD1.BCH_CDE 
          AND LBD1.BEGNDT <= D_DATADATE AND LBD1.OVERDT > D_DATADATE AND LBD1.PARTID = V_PARTID
    LEFT JOIN OMI.PL_BCH_DEPT_HS LBD --个贷机构表
           ON L.APPL_DRAWDN_DISB_AC_BCH=LBD.BCH_CDE 
          AND LBD.BEGNDT <= D_DATADATE AND LBD.OVERDT > D_DATADATE AND LBD.PARTID = V_PARTID	  
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ --机构参数表
           ON LBD.COREBANK_BCH=KJ.JIGOUHAO 
          AND KJ.BEGNDT <= D_DATADATE AND KJ.OVERDT > D_DATADATE AND KJ.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ1 --机构参数表
           ON LBD1.COREBANK_BCH=KJ1.JIGOUHAO 
          AND KJ1.BEGNDT <= D_DATADATE AND KJ1.OVERDT > D_DATADATE AND KJ1.PARTID = V_PARTID
    LEFT JOIN SUBQUERY_6 TMP2 --子查询
           ON TMP2.LOAN_LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_7 TMP3 --子查询
           ON TMP3.LOAN_LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_8 TMP4 --子查询
           ON TMP4.LOAN_LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_9 S9 --子查询
           ON S9.LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_10 S10 --子查询
           ON S10.LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_11 S11 --子查询
           ON S11.LOAN_NO=A.LOAN_NO
    LEFT JOIN SUBQUERY_12 S12 --子查询
           ON S12.LOAN_NO=A.LOAN_NO
	LEFT JOIN SUBQUERY_13 S13 
           ON S13.LOAN_NO=A.LOAN_NO
    LEFT JOIN  urdm.sjbs_dkdbfs_ls C --临时表
           ON C.LOAN_NO=A.LOAN_NO 
    LEFT JOIN  urdm.dkdbfs_ls_JS CJS --临时表 金数用
           ON CJS.LOAN_NO=A.LOAN_NO 
    LEFT JOIN OMI.PL_LOAN_TARGET_HS LT
           ON LT.CODE = A.LOAN_TARGET
          AND LT.BEGNDT <= D_DATADATE AND LT.OVERDT > D_DATADATE AND LT.PARTID = V_PARTID 
	LEFT JOIN (SELECT ST.LOAN_NO FROM OMI.PL_LOAN_SCHD_TERM_HS ST
                WHERE ST.BEGNDT <= D_DATADATE AND ST.OVERDT > D_DATADATE AND ST.PARTID = V_PARTID
                  AND ST.REPAYM_OPT = 'IO'
                GROUP BY ST.LOAN_NO) ST
           ON A.LOAN_NO = ST.LOAN_NO
    LEFT JOIN M_DICT_REMAPPING_DL M --码值映射表
           ON A.PRPS_FIN=M.ORI_VALUES 
          AND M.BUSINESS_TYPE='RSUM' 
          AND M.DICT_CODE='PL-PRPS_FIN-GENERAL_RESERVE'
    LEFT JOIN M_DICT_REMAPPING_DL M1 --码值映射表
           ON A.CUST_REF=M1.ORI_VALUES 
          AND M1.BUSINESS_TYPE='RSUM' 
          AND M1.DICT_CODE='CL-CUST_REF-CAMPUS_CONSU_LOAN_FLG'
    LEFT JOIN M_DICT_REMAPPING_DL M3 --码值映射表
           ON A.ENDOWMENT_INS_NO = REPLACE(M3.ORI_VALUES,'%','')
          AND M3.BUSINESS_TYPE='RSUM' 
          AND M3.DICT_CODE='PL-ENDOWMENT_INS_NO-GREEN_PURPOSE_CD'
    LEFT JOIN URDM.PL_LBI_R_LOAN_DETAIL_HS  R1
      ON A.LOAN_NO=R1.LOAN_NO
       AND R1.RPT_DT=D_DATADATE 
	LEFT JOIN (
        select count(*) cnt,loan_no from omi.pl_RSTR_LOg_hs  rl where rl.rstr_typ="TN"
                 and rl.create_dt>=TO_DATE(to_char(D_DATADATE, 'yyyy-MM-01')) 
                 and rl.create_dt<=D_DATADATE
               GROUP BY loan_no
             )rll  --与老金数核对新增 20230531
		ON A.LOAN_NO = rll.loan_no	   	   
    WHERE A.STS NOT IN ('REVS','NBAP')  --REVS 冲正  NBAP 未放款
      AND (A.STS <> 'SETL' OR (A.STS = 'SETL' AND 
      CASE WHEN A.STS='SETL' AND A.DEBT_STS ='NORM' THEN TO_DATE(NVL(S12.VAL_DT,A.LAST_SETL_DT))
           WHEN A.STS='SETL' AND A.DEBT_STS ='CHRGO' THEN TO_DATE(GB.LAST_APPR_DT) 
        END IS NOT NULL))
      AND A.ORIG_PRCP > 0 --放款金额
      --AND a.loan_no <> '109980001690100'  --历史垃圾数据
      AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID;
	
	V_DATA_COUNT := SQL%ROWCOUNT;
    V_STEP_FLAG := 1;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);
    
    V_STEP_ID   := V_STEP_ID + 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '第3段网贷加工逻辑';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

    INSERT INTO L_ACCT_LOAN 
    (ACCT_NUM,  --合同号
     ACCT_STS, 
     ACCT_STS_DESC,  --账户状态说明
     ACCT_TYP,  --账户类型
     ACCT_TYP_DESC,  --账户类型说明
     ACCU_INT_AMT,  --应计利息
     ACCU_INT_FLG,  --计息标志
     ACTUAL_MATURITY_DT,  --实际到期日期
     BASE_INT_RAT,  --基准利率
     BOOK_TYPE,  --账户种类
     CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
     CANCEL_FLG,  --核销标志
     CIRCLE_LOAN_FLG,  --循环贷款标志
     COMP_INT_TYP,  --贴息类型
     CONSECUTE_TERM_NUM,  --连续欠款期数
     CONSUME_LOAN_ADD_TYPE,  --消费贷款补充细类
     CUMULATE_TERM_NUM,  --累计欠款期数
     CURRENT_TERM_NUM,  --当前还款期数
     CURR_CD,  --币种
     CUST_ID,  --客户号
     DATA_DATE,  --数据日期
     DATE_SOURCESD,  --数据来源
     DEPARTMENTD,  --归属部门
     DRAWDOWN_AMT,  --放款金额
     DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
     DRAWDOWN_DT,  --放款日期
     DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
     DRAWDOWN_TYPE,  --放款方式
     ENTRUST_PAY_AMT,  --受托支付金额
     EXTENDTERM_FLG,  --展期标志
     FINISH_DT,  --结清日期
     FLOAT_TYPE,  --参考利率类型
     FUND_USE_LOC_CD,  --贷款资金使用位置
     GENERALIZE_LOAN_FLG,  --普惠型贷款标志
     GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
     GREEN_LOAN_FLG,  --绿色贷款标志
     GUARANTY_TYP,  --贷款担保方式
     HOUSMORT_LOAN_TYP,  --是否个人住房抵押追加贷款
     INDEPENDENCE_PAY_AMT,  --自主支付金额
     INTERNET_LOAN_FLG,  --互联网贷款标志
     INT_ADJEST_AMT,  --利息调整
     INT_RATE_TYP,  --利率类型
     INT_REPAY_FREQ,  --利息还款频率
     IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
     IS_REPAY_OPTIONS,  --是否内嵌提前还款权
     ITEM_CD,  --科目号
     I_OD_DT,  --利息逾期日期
     LOAN_ACCT_BAL,  --贷款余额
     LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
     LOAN_ACCT_NAME,  --贷款入账账户名称
     LOAN_ACCT_NUM,  --贷款入账账号
     LOAN_BUSINESS_TYP,  --贷款用途分类
     LOAN_BUY_INT,  --是否转入贷款
     LOAN_FHZ_NUM,  --贷款分户账账号
     LOAN_GRADE_CD,  --五级分类代码
     LOAN_NUM,  --贷款编号
     LOAN_PURPOSE_AREA,  --贷款投向地区
     LOAN_PURPOSE_CD,  --贷款投向
     LOAN_PURPOSE_COUNTRY,  --贷款投向国别
     LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
     LOAN_RATIO,  --出资比例
     LOAN_SELL_INT,  --转出标志
     LOW_RISK_FLAG,  --是否低风险业务
     MAIN_GUARANTY_TYP,  --主要担保方式
     MATURITY_DT,  --原始到期日期
     YEAR_INT_INCOM,  --本年利息收入
     NEXT_INT_PAY_DT,  --下一付息日
     NEXT_REPRICING_DT,  --下一利率重定价日
     OD_DAYS,  --逾期天数
     OD_FLG,  --逾期标志
     OD_INT,  --逾期利息
     OD_INT_OBS,  --表外欠息
     OD_LOAN_ACCT_BAL,  --逾期贷款余额
     ORG_NUM,  --机构号
     ORIG_TERM,  --原始期限
     ORIG_TERM_TYPE,  --原始期限类型
     OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
     PAY_ACCT_BANK,  --还款账号所属行名称
     PAY_ACCT_NUM,  --还款账号
     PFTZ_LOAN_FLG,  --自贸区贷款标志
     PRICING_BASE_TYPE,  --定价基础类型
     P_OD_DT,  --本金逾期日期
     REAL_INT_RAT,  --实际利率
     RENEW_FLG,  --无还本续贷标志
     REPAY_FLG,  --借新还旧标志
     REPAY_TERM_NUM,  --还款总期数
     REPAY_TYP,  --还款方式_2
     REPAY_TYP_DESC,  --还款方式说明
     RESCHED_FLG,  --重组标志
     TAX_RELATED_FLG,  --银税合作贷款标志
     TRANSFERS_LOAN_FLG,  --转贷款标志
     USEOFUNDS,  --贷款用途
     YANGTZE_RIVER_LOAN_FLG  --长江经济带贷款标志
    )
    WITH SUBQUERY_1 AS 
            (SELECT KEHUZHAO,ZHANGHAO,
                    ROW_NUMBER() OVER(PARTITION BY KEHUZHAO ORDER BY ZHHAOXUH) NUM
             FROM OMI.KN_KDPA_ZHDUIZ_HS KZ 
            WHERE KZ.BEGNDT <= D_DATADATE AND KZ.OVERDT > D_DATADATE AND KZ.PARTID = V_PARTID),
         SUBQUERY_2 AS 
            (SELECT APS.RELATIVEOBJECTNO,NVL(MIN(APS.PAYDATE),'99991231') AS PAYDATE
             FROM OMI.NL_ACCT_PAYMENT_SCHEDULE_HS APS
             WHERE APS.BEGNDT <= D_DATADATE AND APS.OVERDT > D_DATADATE AND APS.PARTID = V_PARTID
             AND REPLACE (APS.PAYDATE,'/','') <= TO_CHAR(D_DATADATE,'YYYYMMDD')
             --AND APS.STATUS='2'--逾期 
             AND APS.PAYPRINCIPALAMT -APS.ACTUALPAYPRINCIPALAMT>0
             GROUP BY APS.RELATIVEOBJECTNO),
         SUBQUERY_3 AS 
            (SELECT APS.RELATIVEOBJECTNO,NVL(MIN(APS.PAYDATE),'99991231') AS PAYDATE
             FROM OMI.NL_ACCT_PAYMENT_SCHEDULE_HS APS
             WHERE APS.BEGNDT <= D_DATADATE AND APS.OVERDT > D_DATADATE AND APS.PARTID = V_PARTID
             AND REPLACE (APS.PAYDATE,'/','') <= TO_CHAR(D_DATADATE,'YYYYMMDD')
             --AND APS.STATUS='2'--逾期 
             AND APS.PAYINTERESTAMT-APS.ACTUALPAYINTERESTAMT+APS.PAYINTERESTPENALTYAMT-APS.ACTUALPAYINTERESTPENALTYAMT+APS.PAYPRINCIPALPENALTYAMT-APS.ACTUALPAYPRINCIPALPENALTYAMT>0 --应还利息-实还利息+应还利息罚息-实还利息罚息+应还本金罚息-实还本金罚息>0
             GROUP BY APS.RELATIVEOBJECTNO),
         SUBQUERY_4 AS 
            (SELECT MAX(APS.PERIODNO) AS PERIODNO,APS.RELATIVEOBJECTNO
             FROM OMI.NL_ACCT_PAYMENT_SCHEDULE_HS APS, OMI.NL_ACCT_LOAN_HS A
             WHERE APS.BEGNDT <= D_DATADATE AND APS.OVERDT > D_DATADATE AND APS.PARTID = V_PARTID
               AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
               AND APS.RELATIVEOBJECTTYPE = 'jbo.acct.ACCT_LOAN'
               AND APS.RELATIVEOBJECTNO = A.SERIALNO
               AND A.LOANSTATUS IN ('2', '3', '4', '9')
             GROUP BY APS.RELATIVEOBJECTNO),
         SUBQUERY_5 AS 
            (SELECT MIN(APS.PERIODNO) AS PERIODNO,APS.RELATIVEOBJECTNO
             FROM OMI.NL_ACCT_PAYMENT_SCHEDULE_HS APS, OMI.NL_ACCT_LOAN_HS A
             WHERE APS.BEGNDT <= D_DATADATE AND APS.OVERDT > D_DATADATE AND APS.PARTID = V_PARTID
               AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
              AND APS.RELATIVEOBJECTTYPE = 'jbo.acct.ACCT_LOAN'
              AND APS.RELATIVEOBJECTNO = A.SERIALNO
              AND REPLACE(APS.PAYDATE, '/', '') >= TO_CHAR(D_DATADATE,'YYYYMMDD')
              AND A.LOANSTATUS IN ('0', '1')
             GROUP BY APS.RELATIVEOBJECTNO),
         SUBQUERY_6 AS 
            (SELECT COUNT(APS.SERIALNO) AS CT, APS.RELATIVEOBJECTNO
             FROM OMI.NL_ACCT_PAYMENT_SCHEDULE_HS APS
             INNER JOIN OMI.NL_ACCT_LOAN_HS A
                     ON APS.RELATIVEOBJECTNO = A.SERIALNO
                    AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
             WHERE APS.BEGNDT <= D_DATADATE AND APS.OVERDT > D_DATADATE AND APS.PARTID = V_PARTID
               AND APS.RELATIVEOBJECTTYPE = 'jbo.acct.ACCT_LOAN'
               AND APS.FINISHDATE IS NULL
               AND (REPLACE(APS.PAYDATE, '/', '') <= TO_CHAR(D_DATADATE,'YYYYMMDD') OR A.OVERDUEINTEREST > 0)
             GROUP BY APS.RELATIVEOBJECTNO),
         SUBQUERY_7 AS
            (SELECT APL1.RELATIVEOBJECTNO,SUM(CASE WHEN SUBSTR(APL1.ACTUALPAYDATE,1,4) = SUBSTR(TO_CHAR(D_DATADATE,'YYYYMMDD'),1,4) THEN 
		          APL1.ACTUALPAYINTERESTAMT+APL1.ACTUALPAYPRINCIPALPENALTYAMT+APL1.ACTUALPAYINTERESTPENALTYAMT
		          ELSE 0 END) AS BNLXSR 
		  FROM OMI.NL_ACCT_PAYMENT_LOG_HS APL1 --贷款-还款记录
         WHERE APL1.BEGNDT <= D_DATADATE AND APL1.OVERDT > D_DATADATE AND APL1.PARTID = V_PARTID
		 GROUP BY APL1.RELATIVEOBJECTNO)
    SELECT D.SERIALNO AS ACCT_NUM,  --合同号
        CASE WHEN A.LOANSTATUS = '0' THEN '1'
        WHEN A.LOANSTATUS = '1' THEN '2'
        -- WHEN A.LOANSTATUS IN ('2', '3', '4', '9') THEN '3'
        WHEN A.LOANSTATUS IN ('6','7','2', '3', '4', '9') THEN '3' --modify by SPE-20241218-0032
        ELSE '9'
        END AS ACCT_STS,  --账户状态
        CASE WHEN A.LOANSTATUS NOT IN ('0','1','2','3','4','9') THEN '其他'
        END AS ACCT_STS_DESC,  --账户状态说明
        CASE WHEN A.PRODUCTID = '1010010' THEN '010299'
        WHEN A.PRODUCTID = '1030010' THEN '010399'
        ELSE'010399'
        END AS ACCT_TYP,  --账户类型
        CASE WHEN A.PRODUCTID = '1010010' THEN '个人经营性贷款'
        WHEN A.PRODUCTID = '1030010' THEN '个人消费性贷款'
        ELSE '互联网贷款'
        END AS ACCT_TYP_DESC,  --账户类型说明
        A.ACCRUEDINTEREST AS ACCU_INT_AMT,  --应计利息
        'Y' AS ACCU_INT_FLG,  --计息标志
        TDH_TODATE(NVL(A.FINISHDATE,A.MATURITYDATE)) AS ACTUAL_MATURITY_DT,  --实际到期日期
        F.BASERATE AS BASE_INT_RAT,  --基准利率
        '2' AS BOOK_TYPE,  --账户种类
        'N' AS CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
        'N' AS CANCEL_FLG,  --核销标志
        'N' AS CIRCLE_LOAN_FLG,  --循环贷款标志
        'N' AS COMP_INT_TYP,  --贴息类型
        NVL(CASE WHEN A.LOANSTATUS = '1' THEN LJQKQS.CT END,0) AS CONSECUTE_TERM_NUM,  --连续欠款期数
        CASE WHEN D.PURPOSETYPE='4' THEN  'B'
        WHEN D.PURPOSETYPE='5' THEN 'A'
        ELSE '' END AS CONSUME_LOAN_ADD_TYPE,  --消费贷款补充细类
        CASE WHEN NVL(CASE WHEN A.LOANSTATUS <> '6' THEN LJQKQS.CT END, 0) < NVL(CASE WHEN A.LOANSTATUS = '1' THEN LJQKQS.CT END,0) THEN NVL(CASE WHEN A.LOANSTATUS = '1' THEN LJQKQS.CT END,0)
        ELSE NVL(CASE WHEN A.LOANSTATUS <> '6' THEN LJQKQS.CT END,0)
        END AS CUMULATE_TERM_NUM,  --累计欠款期数
        NVL(NVL(AA.PERIODNO, BB.PERIODNO), 0) AS CURRENT_TERM_NUM,  --当前还款期数
        'CNY' AS CURR_CD,  --币种
        NVL(NVL(I.KEHUHAOO,B.MFCUSTOMERID),A.CUSTOMERID) AS CUST_ID,  --客户号
        to_char(D_DATADATE,'YYYYMMDD') AS DATA_DATE,  --数据日期
        'NL_3' AS DATE_SOURCESD,  --数据来源
        'xwdk' AS DEPARTMENTD,  --归属部门
        NVL(A.BUSINESSSUM, 0) AS DRAWDOWN_AMT,  --放款金额
        F.BASERATE AS DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
        TDH_TODATE(A.PUTOUTDATE) AS DRAWDOWN_DT,  --放款日期
        NVL(F.BUSINESSRATE, 0) AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
        'A' AS DRAWDOWN_TYPE,  --放款方式
        '0' AS ENTRUST_PAY_AMT,  --受托支付金额
        'N' AS EXTENDTERM_FLG,  --展期标志
        TDH_TODATE(A.FINISHDATE) AS FINISH_DT,  --结清日期
        CASE WHEN F.BASERATETYPE = '090' THEN 'C'
        ELSE'C' END AS FLOAT_TYPE,  --参考利率类型
        'I' AS FUND_USE_LOC_CD,  --贷款资金使用位置
        CASE WHEN A.PRODUCTID = '1010010' THEN 'Y'
        WHEN A.PRODUCTID = '1030010' THEN 'N'
        END AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
        'N' AS GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
        'N' AS GREEN_LOAN_FLG,  --绿色贷款标志
        'C99' AS GUARANTY_TYP,  --贷款担保方式
        'N' AS HOUSMORT_LOAN_TYP,  --是否个人住房抵押追加贷款
        NVL(A.BUSINESSSUM, 0) AS INDEPENDENCE_PAY_AMT,  --自主支付金额
        'Y' AS INTERNET_LOAN_FLG,  --互联网贷款标志
        0 AS INT_ADJEST_AMT,  --利息调整
        'F' AS INT_RATE_TYP,  --利率类型
        '03' AS INT_REPAY_FREQ,  --利息还款频率
        'N' AS IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
        'Y' AS IS_REPAY_OPTIONS,  --是否内嵌提前还款权
        CASE WHEN A.PRODUCTID = '1010010' THEN '11050201'
        ELSE '11040401' END AS ITEM_CD,  --科目号
        TDH_TODATE(REPLACE (APS2.PAYDATE,'/','')) AS I_OD_DT,  --利息逾期日期
        CASE WHEN A.LOANSTATUS IN ('2', '3', '4', '9') THEN 0
        ELSE NVL(A.NORMALBALANCE, 0) + NVL(A.OVERDUEBALANCE, 0) END AS LOAN_ACCT_BAL,  --贷款余额
        H.BANKSETTLECODE AS LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
        H.ACCOUNTNAME AS LOAN_ACCT_NAME,  --贷款入账账户名称
        NVL(DZ.ZHANGHAO, H.ACCOUNTNO) AS LOAN_ACCT_NUM,  --贷款入账账号
        '4' AS LOAN_BUSINESS_TYP,  --贷款用途分类
        'N' AS LOAN_BUY_INT,  --是否转入贷款
        A.SERIALNO AS LOAN_FHZ_NUM,  --贷款分户账账号
        TRIM(CASE WHEN E1.ITEMNAME IS NULL THEN '1'
        WHEN  E1.ITEMNAME='正常' THEN '1'
        WHEN E1.ITEMNAME='关注' THEN '2'
        WHEN E1.ITEMNAME='次级' THEN '3'
        WHEN E1.ITEMNAME='可疑' THEN '4'
        WHEN  E1.ITEMNAME='损失' THEN '5' END) AS LOAN_GRADE_CD,  --五级分类代码
        A.SERIALNO AS LOAN_NUM,  --贷款编号
        D.CITY AS LOAN_PURPOSE_AREA,  --贷款投向地区
        D.DIRECTION AS LOAN_PURPOSE_CD,  --贷款投向
        'CHN' AS LOAN_PURPOSE_COUNTRY,  --贷款投向国别
        'CHN' AS LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
        '100%' AS LOAN_RATIO,  --出资比例
        'N' AS LOAN_SELL_INT,  --转出标志
		
		'N' AS LOW_RISK_FLAG,  --是否低风险业务
        '2' AS MAIN_GUARANTY_TYP,  --主要担保方式
        TDH_TODATE(A.ORIGINALMATURITYDATE) AS MATURITY_DT,  --原始到期日期
        APL1.BNLXSR AS YEAR_INT_INCOM,  --本年利息收入
        TDH_TODATE(APS.PAYDATE) AS NEXT_INT_PAY_DT,  --下一付息日
        TDH_TODATE(NVL(A.FINISHDATE,A.MATURITYDATE)) AS NEXT_REPRICING_DT,  --下一利率重定价日
        A.OVERDUEDAYS as OD_DAYS,  --逾期天数
        CASE WHEN A.LOANSTATUS = '1' THEN 'Y'
             ELSE 'N'
        END AS OD_FLG,  --逾期标志
        A.OVERDUEINTEREST AS OD_INT,  --逾期利息
        '0' AS OD_INT_OBS,  --表外欠息
        A.OVERDUEBALANCE AS OD_LOAN_ACCT_BAL,  --逾期贷款余额
        C.MAINFRAMEORGID AS ORG_NUM,  --机构号
        D.BUSINESSTERMMONTH AS ORIG_TERM,  --原始期限
        'M' AS ORIG_TERM_TYPE,  --原始期限类型
        'N' AS OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
        H.BANKSETTLECODE AS PAY_ACCT_BANK,  --还款账号所属行名称
        NVL(DZ.ZHANGHAO, H.ACCOUNTNO) AS PAY_ACCT_NUM,  --还款账号
        'N' AS PFTZ_LOAN_FLG,  --自贸区贷款标志
        TRIM(CASE WHEN F.BASERATETYPE = '090' THEN 'C'
             ELSE'Z99'
        END) ASPRICING_BASE_TYPE,  --定价基础类型
        TDH_TODATE(REPLACE (APS1.PAYDATE,'/','')) AS P_OD_DT,  --本金逾期日期
        NVL(F.BUSINESSRATE, 0) AS REAL_INT_RAT,  --实际利率
        'N' AS RENEW_FLG,  --无还本续贷标志
        'N' AS REPAY_FLG,  --借新还旧标志
        D.BUSINESSTERMMONTH AS REPAY_TERM_NUM,  --还款总期数
        CASE WHEN G.TERMID = 'RPT-01' THEN'1'
          WHEN G.TERMID = 'RPT-04' THEN'5'
          WHEN G.TERMID = 'RPT-03' THEN'6'
          ELSE '7' 
        END AS REPAY_TYP,  --还款方式_2
        CASE WHEN G.TERMID NOT IN('RPT-01','RPT-04','RPT-03')THEN '其他'
          ELSE '' 
        END AS REPAY_TYP_DESC,  --还款方式说明
        'N' AS RESCHED_FLG,  --重组标志
        'N' AS TAX_RELATED_FLG,  --银税合作贷款标志
        'N' AS TRANSFERS_LOAN_FLG,  --转贷款标志
        CL.ITEMNAME AS USEOFUNDS,  --贷款用途
        'N' AS YANGTZE_RIVER_LOAN_FLG  --长江经济带贷款标志
    FROM OMI.NL_ACCT_LOAN_HS A --借据信息
    LEFT JOIN OMI.NL_BUSINESS_CONTRACT_CFS_HS D --合同信息
           ON A.CONTRACTSERIALNO = D.SERIALNO 
          AND D.BEGNDT <= D_DATADATE AND D.OVERDT > D_DATADATE AND D.PARTID = V_PARTID
    LEFT JOIN OMI.NL_ORG_INFO_CFS_HS C --机构信息
           ON A.ACCOUNTINGORGID = C.ORGID 
          AND C.BEGNDT <= D_DATADATE AND C.OVERDT > D_DATADATE AND C.PARTID = V_PARTID
    LEFT JOIN OMI.NL_CUSTOMER_INFO_CFS_HS B --客户表
           ON A.CUSTOMERID = B.CUSTOMERID 
          AND B.BEGNDT <= D_DATADATE AND B.OVERDT > D_DATADATE AND B.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KCFB_CFDSJC_HS I --对私客户基本信息表
           ON B.CERTID = I.ZHJHAOMA 
          AND I.BEGNDT <= D_DATADATE AND I.OVERDT > D_DATADATE AND I.PARTID = V_PARTID
    LEFT JOIN OMI.NL_CODE_LIBRARY_CFS_HS E1 --代码库
           ON A.CLASSIFYRESULT = E1.ITEMNO AND E1.CODENO = 'ClassifyResult' 
          AND E1.BEGNDT <= D_DATADATE AND E1.OVERDT > D_DATADATE AND E1.PARTID = V_PARTID
    LEFT JOIN OMI.NL_CODE_LIBRARY_CFS_HS CL --代码库
           ON D.PURPOSETYPE = CL.ITEMNO AND CL.CODENO = 'CreditPurposeType' 
          AND CL.BEGNDT <= D_DATADATE AND CL.OVERDT > D_DATADATE AND CL.PARTID = V_PARTID
    LEFT JOIN OMI.NL_ACCT_RATE_SEGMENT_HS F --贷款-利率区段表
           ON F.OBJECTNO = A.SERIALNO AND F.OBJECTTYPE = 'jbo.acct.ACCT_LOAN' AND F.RATETYPE = '01' 
          AND F.BEGNDT <= D_DATADATE AND F.OVERDT > D_DATADATE AND F.PARTID = V_PARTID
    LEFT JOIN OMI.NL_ACCT_RPT_SEGMENT_HS G --贷款-组合还款区段表
           ON G.OBJECTNO = A.SERIALNO AND G.OBJECTTYPE = 'jbo.acct.ACCT_LOAN' 
          AND G.BEGNDT <= D_DATADATE AND G.OVERDT > D_DATADATE AND G.PARTID = V_PARTID
       
    LEFT JOIN OMI.NL_ACCT_BUSINESS_ACCOUNT_HS H --业务账号信息
           ON H.OBJECTNO = A.SERIALNO AND H.OBJECTTYPE = 'jbo.acct.ACCT_LOAN' 
          AND H.BEGNDT <= D_DATADATE AND H.OVERDT > D_DATADATE AND H.PARTID = V_PARTID  
    LEFT JOIN (SELECT APS.RELATIVEOBJECTNO,MIN(PAYDATE) AS PAYDATE 
                 FROM OMI.NL_ACCT_PAYMENT_SCHEDULE_HS APS --贷款-还款计划
                WHERE APS.BEGNDT <= D_DATADATE AND APS.OVERDT > D_DATADATE AND APS.PARTID = V_PARTID
                  AND REPLACE(PAYDATE,'/','') >=TO_CHAR(D_DATADATE,'YYYYMMDD')
                GROUP BY APS.RELATIVEOBJECTNO) APS     
           ON APS.RELATIVEOBJECTNO = A.SERIALNO 
    LEFT JOIN SUBQUERY_7 APL1 --贷款-还款记录
           ON APL1.RELATIVEOBJECTNO = A.SERIALNO 
    LEFT JOIN SUBQUERY_1 DZ --子查询
           ON H.ACCOUNTNO = DZ.KEHUZHAO AND DZ.NUM = 1   
    LEFT JOIN SUBQUERY_2 APS1 --子查询
           ON APS1.RELATIVEOBJECTNO = A.SERIALNO
    LEFT JOIN SUBQUERY_3 APS2 --子查询
           ON APS2.RELATIVEOBJECTNO = A.SERIALNO
    LEFT JOIN SUBQUERY_4 AA --子查询
           ON AA.RELATIVEOBJECTNO = A.SERIALNO
    LEFT JOIN SUBQUERY_5 BB --子查询
           ON BB.RELATIVEOBJECTNO = A.SERIALNO
    LEFT JOIN SUBQUERY_6 LJQKQS --子查询
           ON LJQKQS.RELATIVEOBJECTNO = A.SERIALNO
    WHERE A.LOANSTATUS <> '6'  --发放失败
      AND A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID;

    V_DATA_COUNT := SQL%ROWCOUNT;
    V_STEP_FLAG := 1;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);
	
	V_STEP_ID   := V_STEP_ID + 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '第4段税金贷加工逻辑';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

    INSERT INTO L_ACCT_LOAN (ACCT_NUM,  --合同号
     ACCT_STS,  --账户状态
     ACCT_STS_DESC,  --账户状态说明
     ACCT_TYP,  --账户类型
     ACCT_TYP_DESC,  --账户类型说明
     ACCU_INT_AMT,  --应计利息
     ACCU_INT_FLG,  --计息标志
     ACTUAL_MATURITY_DT,  --实际到期日期
     BASE_INT_RAT,  --基准利率
     BOOK_TYPE,  --账户种类
     CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
     CANCEL_FLG,  --核销标志
     CIRCLE_LOAN_FLG,  --循环贷款标志
     COMP_INT_TYP,  --贴息类型
     CONSECUTE_TERM_NUM,  --连续欠款期数
     CUMULATE_TERM_NUM,  --累计欠款期数
     CURRENT_TERM_NUM,  --当前还款期数
     CURR_CD,  --币种
     CUST_ID,  --客户号
     DATA_DATE,  --数据日期
     DATE_SOURCESD,  --数据来源
     DEPARTMENTD,  --归属部门
     DRAWDOWN_AMT,  --放款金额
     DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
     DRAWDOWN_DT,  --放款日期
     DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
     DRAWDOWN_TYPE,  --放款方式
     EMP_ID,  --信贷员员工号
     EXTENDTERM_FLG,  --展期标志
     FINISH_DT,  --结清日期
     FLOAT_TYPE,  --参考利率类型
     FUND_USE_LOC_CD,  --贷款资金使用位置
     GENERALIZE_LOAN_FLG,  --普惠型贷款标志
     GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
     GREEN_BALANCE,  --绿色信贷余额
     GREEN_LOAN_FLG,  --绿色贷款标志
     GREEN_LOAN_TYPE,  --绿色贷款用途分类
     GREEN_PURPOSE_CD,  --绿色融资投向
     GUARANTY_TYP,  --贷款担保方式
     INDEPENDENCE_PAY_AMT,  --自主支付金额
     INTERNET_LOAN_FLG,  --互联网贷款标志
     INT_ADJEST_AMT,  --利息调整
     INT_RATE_TYP,  --利率类型
     INT_REPAY_FREQ,  --利息还款频率
     IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
     IS_REPAY_OPTIONS,  --是否内嵌提前还款权
     ITEM_CD,  --科目号
     I_OD_DT,  --利息逾期日期
     LOAN_ACCT_BAL,  --贷款余额
     LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
     LOAN_ACCT_NAME,  --贷款入账账户名称
     LOAN_ACCT_NUM,  --贷款入账账号
     LOAN_BUSINESS_TYP,  --贷款用途分类
     LOAN_BUY_INT,  --是否转入贷款
     LOAN_FHZ_NUM,  --贷款分户账账号
     LOAN_GRADE_CD,  --五级分类代码
     LOAN_NUM,  --贷款编号
     LOAN_PURPOSE_AREA,  --贷款投向地区
     LOAN_PURPOSE_CD,  --贷款投向
     LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
     LOAN_RATIO,  --出资比例
     LOAN_SELL_INT,  --转出标志
     LOW_RISK_FLAG,  --是否低风险业务
     MAIN_GUARANTY_TYP,  --主要担保方式
     MATURITY_DT,  --原始到期日期
     YEAR_INT_INCOM,  --本年利息收入
     NEXT_INT_PAY_DT,  --下一付息日
     NEXT_REPRICING_DT,  --下一利率重定价日
     OD_DAYS,  --逾期天数
     OD_FLG,  --逾期标志
     OD_INT,  --逾期利息
     OD_INT_OBS,  --表外欠息
     OD_LOAN_ACCT_BAL,  --逾期贷款余额
     ORG_NUM,  --机构号
     ORIG_TERM,  --原始期限
     ORIG_TERM_TYPE,  --原始期限类型
     OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
     PAY_ACCT_BANK,  --还款账号所属行名称
     PAY_ACCT_NUM,  --还款账号
     POVERTY_ALLE,  --已脱贫人口贷款标志
     PPL_REPAY_FREQ,  --本金还款频率
     PRICING_BASE_TYPE,  --定价基础类型
     P_OD_DT,  --本金逾期日期
     REAL_INT_RAT,  --实际利率
     RENEW_FLG,  --无还本续贷标志
     REPAY_FLG,  --借新还旧标志
     REPAY_TERM_NUM,  --还款总期数
     REPAY_TYP,  --还款方式_2
     RESCHED_END_DT,  --观察期到期日期
     RESCHED_FLG,  --重组标志
     TAX_RELATED_FLG,  --银税合作贷款标志
     TRANSFERS_LOAN_FLG,  --转贷款标志
     USEOFUNDS,  --贷款用途
     YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志
	 OD_TYPE_JS  --逾期类型
    )
    WITH SUBQUERY_1 AS 
            (SELECT SUM(I11.SUB_BAL) SUB_BAL, I11.DISB_NO
             FROM OMI.WBYXNL_AMS_L_IOU_BAL_HS I11
             WHERE I11.BEGNDT <= D_DATADATE AND I11.OVERDT > D_DATADATE AND I11.PARTID = V_PARTID
              AND I11.SUB_CODE = '12010132'
              AND I11.SUB_CODE_ORDER = 3
             GROUP BY I11.DISB_NO),
         SUBQUERY_2 AS 
            (SELECT SUM(I21.SUB_BAL) SUB_BAL, I21.DISB_NO
             FROM OMI.WBYXNL_AMS_L_IOU_BAL_HS I21
             WHERE I21.BEGNDT <= D_DATADATE AND I21.OVERDT > D_DATADATE AND I21.PARTID = V_PARTID
               AND I21.SUB_CODE = '721102'
               AND I21.SUB_CODE_ORDER = 3
             GROUP BY I21.DISB_NO),
         SUBQUERY_3 AS 
            (SELECT KKZZ.KEHUZHAO,KKZZ.ZHANGHAO,
                    ROW_NUMBER() OVER(PARTITION BY KKZZ.KEHUZHAO ORDER BY KKZZ.ZHHAOXUH) NUM
             FROM OMI.KN_KDPA_ZHDUIZ_HS KKZZ 
            WHERE KKZZ.BEGNDT <= D_DATADATE AND KKZZ.OVERDT > D_DATADATE AND KKZZ.PARTID = V_PARTID),
         SUBQUERY_4 AS 
            (SELECT  KKZZ1.KEHUZHAO,KKZZ1.ZHANGHAO,
                     ROW_NUMBER() OVER(PARTITION BY KKZZ1.KEHUZHAO ORDER BY KKZZ1.ZHHAOXUH) NUM
             FROM OMI.KN_KDPA_ZHDUIZ_HS KKZZ1
            WHERE KKZZ1.BEGNDT <= D_DATADATE AND KKZZ1.OVERDT > D_DATADATE AND KKZZ1.PARTID = V_PARTID),
         SUBQUERY_5 AS 
            (SELECT COUNT(1) ALL_OVD_TERMS, RE.CREDIT_NUM
             FROM OMI.WBYXNL_G_DLYH_REPAY_PLAN_HS RE
            WHERE RE.BEGNDT <= D_DATADATE AND RE.OVERDT > D_DATADATE AND RE.PARTID = V_PARTID
             GROUP BY RE.CREDIT_NUM)
    SELECT D.CONTRACT_CODE AS ACCT_NUM,  --合同号
        CASE WHEN G.STATUS = 'NOR' THEN '1'
             WHEN G.STATUS = 'CLEAR' THEN '3'
             WHEN G.STATUS = 'OVD' THEN '2'
        ELSE '9' END AS ACCT_STS,  --账户状态
        CASE WHEN G.STATUS NOT IN('NOR', 'CLEAR', 'OVD') THEN '其他'
             ELSE '' END AS ACCT_STS_DESC,  --账户状态说明
        '010299' AS ACCT_TYP,  --账户类型
        '税闪贷' AS ACCT_TYP_DESC,  --账户类型说明
        --G.INT_BAL+G.OVD_INT_BAL AS ACCU_INT_AMT,  --应计利息
        G.INT_BAL+G.OVD_INT_BAL+G.OVD_PRIN_PNLT_BAL+G.OVD_INT_PNLT_BAL AS ACCU_INT_AMT,  --应计利息
        'Y' AS ACCU_INT_FLG,  --计息标志
        TO_DATE(G.CLEAR_DATE) AS ACTUAL_MATURITY_DT,  --实际到期日期
        D.BASE_RATE AS BASE_INT_RAT,  --基准利率
        '2' AS BOOK_TYPE,  --账户种类
        'N' AS CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
        CASE WHEN G.STATUS = 'CLEAR' THEN 'Y'
        ELSE 'N' END AS CANCEL_FLG,  --核销标志
        --'N' AS CIRCLE_LOAN_FLG,  --循环贷款标志
        'Y' AS CIRCLE_LOAN_FLG,  --循环贷款标志    --by gjw 20231025  根据上报结果推断税闪贷为Y  ???
        'N' AS COMP_INT_TYP,  --贴息类型
        NVL(G.OVD_TERMS, 0) AS CONSECUTE_TERM_NUM,  --连续欠款期数
        NVL(OV.ALL_OVD_TERMS, 0) AS CUMULATE_TERM_NUM,  --累计欠款期数
        NVL(G.CURRENT_TERMS, 0) AS CURRENT_TERM_NUM,  --当前还款期数
        'CNY' AS CURR_CD,  --币种
        G.CUST_NO AS CUST_ID,  --客户号
        to_char(D_DATADATE,'YYYYMMDD') AS DATA_DATE,  --数据日期
        'WBYXNL_4' AS DATE_SOURCESD,  --数据来源
        'xwdk' AS DEPARTMENTD,  --归属部门
        NVL(G.ENCASH_AMT, 0) AS DRAWDOWN_AMT,  --放款金额
        D.BASE_RATE AS DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
        TO_DATE(G.ENCASH_DATE) AS DRAWDOWN_DT,  --放款日期
        G.YEAR_RATE * 100 AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
        'A' AS DRAWDOWN_TYPE,  --放款方式
        O.CUSTOMER_MANAGER_NUMBER AS EMP_ID,  --信贷员员工号
        'N' AS EXTENDTERM_FLG,  --展期标志
        TO_DATE(G.CLEAR_DATE) AS FINISH_DT,  --结清日期
        CASE WHEN G.RATE_TYPE = 'F' THEN 'C' ELSE 'Z99' END AS FLOAT_TYPE,  --参考利率类型
        'I' AS FUND_USE_LOC_CD,  --贷款资金使用位置
        'Y' AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
        'N' AS GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
        CASE 
          WHEN D.IS_GREEN_LOAN ='Y' AND G.STATUS = 'CLEAR' THEN 0
          WHEN D.IS_GREEN_LOAN ='Y' AND G.STATUS <> 'CLEAR' THEN NVL(G.PRIN_BAL, 0) + NVL(G.OVD_PRIN_BAL, 0)
          ELSE ''
        END AS GREEN_BALANCE,  --绿色信贷余额
        NVL(D.IS_GREEN_LOAN,'N') AS GREEN_LOAN_FLG,  --绿色贷款标志
        M2.NEW_VALUES AS GREEN_LOAN_TYPE,  --绿色贷款用途分类
        M1.NEW_VALUES AS GREEN_PURPOSE_CD,  --绿色融资投向
        'D' AS GUARANTY_TYP,  --贷款担保方式
        NVL(G.ENCASH_AMT, 0) AS INDEPENDENCE_PAY_AMT,  --自主支付金额
        'Y' AS INTERNET_LOAN_FLG,  --互联网贷款标志
        0 AS INT_ADJEST_AMT,  --利息调整
       -- 'L5' AS INT_RATE_TYP,  --利率类型
       --和张靖祎确认保持和金数一致 ,默认F wzm 
        'F' AS INT_RATE_TYP,  --利率类型
        '03' AS INT_REPAY_FREQ,  --利息还款频率
        --CASE WHEN LCA.CUST_ID IS NOT NULL THEN 'N' ELSE 'Y' END 
        'N' AS IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
        'Y' AS IS_REPAY_OPTIONS,  --是否内嵌提前还款权
        '11050201' AS ITEM_CD,  --科目号
        CASE 
          WHEN G.STATUS = 'OVD' /*OR G.OVD_INT_BAL >0*/ THEN TO_DATE(G.OVD_DATE)
          ELSE ''
        END AS I_OD_DT,  --利息逾期日期
        CASE 
          WHEN G.STATUS = 'CLEAR' THEN 0
          ELSE NVL(G.PRIN_BAL, 0) + NVL(G.OVD_PRIN_BAL, 0)
        END AS LOAN_ACCT_BAL,  --贷款余额
        NVL(DF.JIGOUZWM,D.DISB_BANK_NAME) AS LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
        D.DISB_ACCT_NAME AS LOAN_ACCT_NAME,  --贷款入账账户名称
        --KZ.ZHANGHAO AS LOAN_ACCT_NUM,  --贷款入账账号
        D.DISB_ACCT AS LOAN_ACCT_NUM,  --贷款入账账号
        '4' AS LOAN_BUSINESS_TYP,  --贷款用途分类
        'N' AS LOAN_BUY_INT,  --是否转入贷款
        D.CONTRACT_CODE AS LOAN_FHZ_NUM,  --贷款分户账账号
        TRIM(CASE 
          WHEN G.ASSET_CLASS IS NULL THEN '1'
          ELSE G.ASSET_CLASS
        END) AS LOAN_GRADE_CD,  --五级分类代码
        G.CREDIT_NUM AS LOAN_NUM,  --贷款编号
        O.REGION_CODE AS LOAN_PURPOSE_AREA,  --贷款投向地区
        TED.HYML_DM||TED.HYXL_DM AS LOAN_PURPOSE_CD,  --贷款投向
        'CHN' AS LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
        '100%' AS LOAN_RATIO,  --出资比例
        'N' AS LOAN_SELL_INT,  --转出标志
        'N' AS LOW_RISK_FLAG,  --是否低风险业务
        '3' AS MAIN_GUARANTY_TYP,  --主要担保方式
        G.END_DATE AS MATURITY_DT,  --原始到期日期
        C.BNLXSY AS YEAR_INT_INCOM,  --本年利息收入
        CASE WHEN g.STATUS in('OVD','CLEAR')  AND to_char(g.NEXT_REPAY_DATE,'yyyymmdd') <= TO_CHAR(D_DATADATE,'YYYYMMDD') THEN  ''
        ELSE TO_DATE(G.NEXT_REPAY_DATE) END AS NEXT_INT_PAY_DT,  --下一付息日
        TO_DATE(G.END_DATE) AS NEXT_REPRICING_DT,  --下一利率重定价日
        CASE 
          WHEN G.STATUS = 'OVD' AND G.PRIN_OVD_DAYS >= G.INT_OVD_DAYS THEN G.PRIN_OVD_DAYS
          WHEN G.STATUS = 'OVD' AND G.PRIN_OVD_DAYS < G.INT_OVD_DAYS THEN G.INT_OVD_DAYS
          ELSE 0
        END AS OD_DAYS,  --逾期天数
        CASE WHEN G.STATUS = 'OVD' THEN 'Y' ELSE 'N' END AS OD_FLG,  --逾期标志
        CASE 
          WHEN G.STATUS = 'NOR' AND (G.OVD_DATE IS NULL OR G.OVD_DATE = 'null') THEN 0
          ELSE NVL(I1.SUB_BAL, 0)
        END AS OD_INT,  --逾期利息
        CASE 
          WHEN G.STATUS = 'NOR' AND (G.OVD_DATE IS NULL OR G.OVD_DATE = 'null') THEN 0
          ELSE NVL(I2.SUB_BAL, 0)
        END AS OD_INT_OBS,  --表外欠息
        G.OVD_PRIN_BAL AS OD_LOAN_ACCT_BAL,  --逾期贷款余额
        G.ORG_NUM AS ORG_NUM,  --机构号
        NVL(G.TOTAL_TERMS, 0) AS ORIG_TERM,  --原始期限
        'M' AS ORIG_TERM_TYPE,  --原始期限类型
        'N' AS OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
        D.DISB_BANK_NAME AS PAY_ACCT_BANK,  --还款账号所属行名称
        --KZ1.ZHANGHAO AS PAY_ACCT_NUM,  --还款账号
        G.REPAY_ACCT_NO AS PAY_ACCT_NUM,  --还款账号
        'N' AS POVERTY_ALLE,  --已脱贫人口贷款标志
        '03' AS PPL_REPAY_FREQ,  --本金还款频率
        TRIM(CASE WHEN G.RATE_TYPE = 'F' THEN 'C' ELSE 'Z99' END) AS PRICING_BASE_TYPE,  --定价基础类型
        CASE 
          WHEN G.STATUS = 'OVD' AND G.OVD_PRIN_BAL>0 THEN G.OVD_DATE
          ELSE ''
        END AS P_OD_DT,  --本金逾期日期
        G.YEAR_RATE * 100 AS REAL_INT_RAT,  --实际利率
        'N' AS RENEW_FLG,  --无还本续贷标志
        'N' AS REPAY_FLG,  --借新还旧标志
        NVL(G.TOTAL_TERMS, 0) AS REPAY_TERM_NUM,  --还款总期数
        '1' AS REPAY_TYP,  --还款方式_2
        ''  AS RESCHED_END_DT,  --观察期到期日期
        'N' AS RESCHED_FLG,  --重组标志
        'Y' AS TAX_RELATED_FLG,  --银税合作贷款标志
        --'N' AS TAX_RELATED_FLG,  --银税合作贷款标志   
        'N' AS TRANSFERS_LOAN_FLG,  --转贷款标志
        '生产经营' AS USEOFUNDS,  --贷款用途
        'N' AS YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志
        CASE WHEN G.STATUS = 'OVD' THEN 
	           CASE WHEN G.ovd_prin_bal > 0 AND G.ovd_int_bal > 0 THEN '03'
	                WHEN G.ovd_prin_bal > 0 THEN '01' 
	            ELSE '02' END 
		ELSE '' END AS OD_TYPE_JS
    FROM OMI.WBYXNL_G_DLYH_LOAN_DETAIL_HS G --借据信息表
    LEFT JOIN OMI.WBYXNL_BUSI_DISB_APPLY_HS D --放款申请表
           ON G.CREDIT_NUM = D.CREDIT_NUM 
          AND D.BEGNDT <= D_DATADATE AND D.OVERDT > D_DATADATE AND D.PARTID = V_PARTID
    LEFT JOIN OMI.WBYXNL_BUSI_ORDER_INFO_HS O --订单表
           ON O.ORDER_CODE = D.ORDER_CODE 
          AND O.BEGNDT <= D_DATADATE AND O.OVERDT > D_DATADATE AND O.PARTID = V_PARTID
    LEFT JOIN OMI.WBYXNL_BUSI_REPORT_DATA_HS R --行里报表相关信息表
           ON G.CREDIT_NUM = R.DISB_APPLY_NO 
          AND R.BEGNDT <= D_DATADATE AND R.OVERDT > D_DATADATE AND R.PARTID = V_PARTID
    LEFT JOIN OMI.WBYXNL_BUSI_ORDER_EXTEND_HS E --订单扩展表
           ON E.ORDER_SEQ = O.ORDER_SEQ 
          AND E.BEGNDT <= D_DATADATE AND E.OVERDT > D_DATADATE AND E.PARTID = V_PARTID
    LEFT JOIN OMI.WBYXNL_TD_ECONOMIC_INFO_HS TED --国民经济行业代码表
           ON E.INDUSTRY_SUB_CODE = TED.HYXL_DM 
          AND TED.BEGNDT <= D_DATADATE AND TED.OVERDT > D_DATADATE AND TED.PARTID = V_PARTID
    LEFT JOIN (SELECT C.CREDIT_NUM,SUM(CASE WHEN SUBSTR(C.REPAY_DATE,1,4) = SUBSTR(TO_CHAR(D_DATADATE, 'YYYYMMDD'),1,4) THEN 
                          C.PAID_INT_AMT+C.PAID_OVD_INT_AMT+C.PAID_OVD_PRIN_PNLT_AMT+C.PAID_OVD_INT_PNLT_AMT
                        ELSE 0 END) AS BNLXSY
                FROM OMI.WBYXNL_G_DLYH_REFUND_DETAIL_HS C --贷款归还信息表
               WHERE C.BEGNDT <= D_DATADATE AND C.OVERDT > D_DATADATE AND C.PARTID = V_PARTID
               GROUP BY C.CREDIT_NUM) C
           ON C.CREDIT_NUM=G.CREDIT_NUM 
    LEFT JOIN OMI.WBYXNL_BUSI_DISB_APPLY_HS BDA --放款申请表
           ON BDA.DISB_APPLY_SEQ=G.CREDIT_NUM 
          AND BDA.BEGNDT <= D_DATADATE AND BDA.OVERDT > D_DATADATE AND BDA.PARTID = V_PARTID
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS DF --机构参数表
           ON DF.JIGOUHAO = D.DISB_BANK_NO
          AND DF.BEGNDT <= D_DATADATE AND DF.OVERDT > D_DATADATE AND DF.PARTID = V_PARTID
    LEFT JOIN SUBQUERY_1 I1 --子查询
           ON I1.DISB_NO = G.CREDIT_NUM
    LEFT JOIN SUBQUERY_2 I2 --子查询
           ON I2.DISB_NO = G.CREDIT_NUM
    LEFT JOIN SUBQUERY_3 KZ --子查询
           ON KZ.KEHUZHAO = D.DISB_ACCT AND KZ.NUM = '1'
    LEFT JOIN SUBQUERY_4 KZ1 --子查询
           ON KZ1.KEHUZHAO = G.REPAY_ACCT_NO AND KZ1.NUM = '1'
    LEFT JOIN SUBQUERY_5 OV --子查询
           ON OV.CREDIT_NUM = G.CREDIT_NUM
    
    LEFT JOIN M_DICT_REMAPPING_DL M2 --码值映射表
           ON D.GREEN_LOAN_PURPOSE=M2.ORI_VALUES AND M2.DICT_CODE='WBYXNL-GREEN_LOAN_PURPOSE-GREEN_LOAN_TYPE' AND M2.BUSINESS_TYPE='RSUM'
    LEFT JOIN M_DICT_REMAPPING_DL M1 --码值映射表
           ON D.YJ_GREEN_LOAN_PURPOSE=M1.ORI_VALUES AND M1.DICT_CODE='WBYXNL-YJ_GREEN_LOAN_PURPOSE-GREEN_PURPOSE_CD' AND M1.BUSINESS_TYPE='RSUM'
    /*LEFT JOIN URDM.L_CUST_ALL LCA --全量客户信息表
           ON G.CUST_NO=LCA.CUST_ID AND LCA.DATA_DATE = 上月末*/      
    WHERE G.BEGNDT <= D_DATADATE AND G.OVERDT > D_DATADATE AND G.PARTID = V_PARTID;

    
    V_DATA_COUNT := SQL%ROWCOUNT;
    V_STEP_FLAG := 1;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);


    V_STEP_ID   := V_STEP_ID + 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '加工L_ACCT_LOAN表-第5段逻辑';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);


	INSERT INTO L_ACCT_LOAN 
	(ACCT_NUM,  --合同号
	 ACCT_STS,  --账户状态
	 ACCT_TYP,  --账户类型
	 ACCU_INT_AMT,  --应计利息
	 ACCU_INT_FLG,  --计息标志
	 ACTUAL_MATURITY_DT,  --实际到期日期
	 BOOK_TYPE,  --账户种类
	 CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
	 CANCEL_FLG,  --核销标志
	 CIRCLE_LOAN_FLG,  --循环贷款标志
	 CONSECUTE_TERM_NUM,  --连续欠款期数
	 CUMULATE_TERM_NUM,  --累计欠款期数
	 CURRENT_TERM_NUM,  --当前还款期数
	 CURR_CD,  --币种
	 CUST_ID,  --客户号
	 DATA_DATE,  --数据日期
	 DATE_SOURCESD,  --数据来源
	 DEPARTMENTD,  --归属部门
	 DISCOUNT_INTEREST,  --贴现利息
	 DRAFT_NBR,  --汇票号码
	 DRAWDOWN_AMT,  --放款金额
	 DRAWDOWN_DT,  --放款日期
	 DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
	 DRAWDOWN_TYPE,  --放款方式
	 EMP_ID,  --信贷员员工号
	 EXTENDTERM_FLG,  --展期标志
	 FINISH_DT,  --结清日期
	 FUND_USE_LOC_CD,  --贷款资金使用位置
	 GENERALIZE_LOAN_FLG,  --普惠型贷款标志
	 SP_PROV_AMT,  --一般准备
	 GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
	 GREEN_BALANCE,  --绿色信贷余额
	 GREEN_LOAN_FLG,  --绿色贷款标志
	 GREEN_LOAN_TYPE,  --绿色贷款用途分类
	 HOUSMORT_LOAN_TYP,  --是否个人住房抵押追加贷款
	 INDUST_TRAN_FLG,  --工业企业技术改造升级标识
	 INTERNET_LOAN_FLG,  --互联网贷款标志
	 INT_ADJEST_AMT,  --利息调整
	 INT_NUM_DATE,  --计息天数
	 INT_RATE_TYP,  --利率类型
	 INT_REPAY_FREQ,  --利息还款频率
	 INT_REPAY_FREQ_DESC,  --利息还款频率说明
	 IS_REPAY_OPTIONS,  --是否内嵌提前还款权
	 ITEM_CD,  --科目号
	 LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
	 LOAN_ACCT_BAL,  --贷款余额
	 LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
	 LOAN_ACCT_NAME,  --贷款入账账户名称
	 LOAN_ACCT_NUM,  --贷款入账账号
	 LOAN_BUSINESS_TYP,  --贷款用途分类
	 LOAN_BUY_INT,  --是否转入贷款
	 LOAN_FHZ_NUM,  --贷款分户账账号
	 LOAN_GRADE_CD,  --五级分类代码
	 LOAN_NUM,  --贷款编号
	 LOAN_PURPOSE_AREA,  --贷款投向地区
	 LOAN_PURPOSE_CD,  --贷款投向
	 LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
	 LOW_RISK_FLAG,  --是否低风险业务
	 MAIN_GUARANTY_TYP,  --主要担保方式
	 MATURITY_DT,  --原始到期日期
	 NEXT_INT_PAY_DT,  --下一付息日
	 NEXT_REPRICING_DT,  --下一利率重定价日
	 OD_DAYS,  --逾期天数
	 OD_FLG,  --逾期标志
	 OD_INT,  --逾期利息
	 OD_INT_OBS,  --表外欠息
	 OD_LOAN_ACCT_BAL,  --逾期贷款余额
	 ORG_NUM,  --机构号
	 ORIG_ACCT_NO,  --原账号
	 OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
	 PAY_ACCT_BANK,  --还款账号所属行名称
	 PFTZ_LOAN_FLG,  --自贸区贷款标志
	 POVERTY_ALLE,  --已脱贫人口贷款标志
	 PPL_REPAY_FREQ,  --本金还款频率
	 PRICING_BASE_TYPE,  --定价基础类型
	 P_OD_DT,  --本金逾期日期
	 REAL_INT_RAT,  --实际利率
	 RENEW_FLG,  --无还本续贷标志
	 REPAY_FLG,  --借新还旧标志
	 REPAY_TERM_NUM,  --还款总期数
	 REPAY_TYP,  --还款方式_2
	 REPAY_TYP_DESC,  --还款方式说明
	 RESCHED_FLG,  --重组标志
	 TAX_RELATED_FLG,  --银税合作贷款标志
	 TRANSFERS_LOAN_FLG,  --转贷款标志
	 USEOFUNDS,  --贷款用途
	 YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志
	 GUARANTY_TYP,  --贷款担保方式
	 LOAN_SELL_INT,  --转出标志
	 ORIG_TERM,  --原始期限
     ORIG_TERM_TYPE,  --原始期限类型
	 ORIG_TERM_BC,	--贷款原始期限-合同
	 ACCT_STS_FHZ, --20240712
	 ACCT_NUM_CQCS --业务合同号客户风险 by:yxy 20250114
	)
	SELECT
	    BCV.CREDIT_NO AS ACCT_NUM,  --合同号
	    CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE <= D_DATADATE THEN '2'
	         ELSE DECODE(ACC.STATUS, '2', '3', '1')
	    END AS ACCT_STS,  --账户状态
	    CASE WHEN BDV.DRAFT_TYPE = '1' THEN '030101'
	         ELSE '030102'
	    END AS ACCT_TYP,  --账户类型
	    0 AS ACCU_INT_AMT,  --应计利息
	    'Y' AS ACCU_INT_FLG,  --计息标志
	    --BDV.PAYMENT_DATE AS ACTUAL_MATURITY_DT,  --实际到期日期
	    BDV.MATURITY_DATE AS ACTUAL_MATURITY_DT,  --实际到期日期  qzj 20240118 解决票据贴现部分到期日期不一致问题
	    '2' AS BOOK_TYPE,  --账户种类
	    'N' AS CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
	    'N' AS CANCEL_FLG,  --核销标志
	    'N' AS CIRCLE_LOAN_FLG,  --循环贷款标志
	    -- 0 AS CONSECUTE_TERM_NUM,  --连续欠款期数
	    CASE WHEN BDV.PAYMENT_DATE <= TO_CHAR(D_DATADATE,'YYYYMMDD') THEN 1 ELSE 0 END AS CONSECUTE_TERM_NUM,  --连续欠款期数  20240711 同生产dgxdywjjb
	    CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE <= D_DATADATE THEN '1'
	         ELSE '0'
	    END AS CUMULATE_TERM_NUM,  --累计欠款期数
	    -- CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE <= D_DATADATE THEN '1'
	         -- ELSE DECODE(ACC.STATUS, '2', '1', '0')
	    -- END AS CURRENT_TERM_NUM,  --当前还款期数
		'1' AS CURRENT_TERM_NUM,  --当前还款期数
	    'CNY' AS CURR_CD,  --币种
	    BCV.CUST_NO AS CUST_ID,  --客户号
	    TO_CHAR(D_DATADATE, 'YYYYMMDD') AS DATA_DATE,  --数据日期
	    'BS_5' AS DATE_SOURCESD,  --数据来源
	    'pjtx' AS DEPARTMENTD,  --归属部门
	    BDV.INTEREST AS DISCOUNT_INTEREST,  --贴现利息
	    CASE WHEN BDV.MIX_FLAG='1' /*OR  BDV.ORG_CD_RANGE='0'*/ THEN BDV.DRAFT_NUMBER 
		     ELSE BDV.DRAFT_NUMBER||','||BDV.ORG_CD_RANGE
	    END AS DRAFT_NBR,  --汇票号码
	    --DECODE(BDV.MIX_FLAG, '1', BDV.DRAFT_NUMBER, BDV.DRAFT_NUMBER || ',' || BDV.ORG_CD_RANGE) AS DRAFT_NBR,  --汇票号码
	    -- BDV.DRAFT_AMOUNT-BDV.INTEREST AS DRAWDOWN_AMT,  --放款金额 20241126 SPE-20240811-0002从票面金额改为实付金额
	     BDV.PAY_AMOUNT AS DRAWDOWN_AMT,  --放款金额 20241126 SPE-20240811-0002从票面金额改为实付金额 用PAY_AMOUNT 而不是票面金额-利息
		--BDV.DRAFT_AMOUNT AS DRAWDOWN_AMT,  --放款金额
	    BTD.ACCT_DATE AS DRAWDOWN_DT,  --放款日期
	    BDV.RATE AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
	    'A' AS DRAWDOWN_TYPE,  --放款方式
	    CASE WHEN upper(BCV.OPERATOR_NO) = 'FASTDIS' THEN BC.MANAGEUSERID
	         ELSE BCV.OPERATOR_NO
	    END AS EMP_ID,  --信贷员员工号
	    /*CASE 
			WHEN ACC.STATUS ='2' AND NVL(ACC.END_DT,TO_CHAR(ACC.UPDATE_TIME,'YYYYMMDD')) < BDV.MATURITY_DATE THEN nvl(cqc.MANAGER_NO,cqc.CREATED_BY) 
		ELSE 

			CASE WHEN upper(BCV.OPERATOR_NO) = 'FASTDIS' THEN BC.MANAGEUSERID
	         ELSE BCV.OPERATOR_NO
			END 
		END AS EMP_ID, */ --信贷员员工号
	    'N' AS EXTENDTERM_FLG,  --展期标志
	    DECODE(ACC.STATUS, '2', ACC.END_DT, '') AS FINISH_DT,  --结清日期
	    'I' AS FUND_USE_LOC_CD,  --贷款资金使用位置
	    'N' AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
	    NVL(I9.ECL_FINAL, 0) AS SP_PROV_AMT,  --一般准备
	    'N' AS GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
	    CASE WHEN NVL(BC.GREENLOANPURPOSE, '0') <> '0' THEN BD.BALANCE
	         ELSE NULL
	    END AS GREEN_BALANCE,  --绿色信贷余额
	    CASE WHEN NVL(BC.GREENLOANPURPOSE, '0') <> '0' THEN 'Y'
	         ELSE 'N'
	    END AS GREEN_LOAN_FLG,  --绿色贷款标志
	    M.NEW_VALUES AS GREEN_LOAN_TYPE,  --绿色贷款用途分类
	    'N' AS HOUSMORT_LOAN_TYP,  --是否个人住房抵押追加贷款
	    '2' AS INDUST_TRAN_FLG,  --工业企业技术改造升级标识
	    'N' AS INTERNET_LOAN_FLG,  --互联网贷款标志
	    0 AS INT_ADJEST_AMT,  --利息调整
	    BDV.PAYMENT_DAYS AS INT_NUM_DATE,  --计息天数
	    'F' AS INT_RATE_TYP,  --利率类型
	    '99' AS INT_REPAY_FREQ,  --利息还款频率
	    '期初一次性扣除利息' AS INT_REPAY_FREQ_DESC,  --利息还款频率说明
	    'Y' AS IS_REPAY_OPTIONS,  --是否内嵌提前还款权
	    DECODE(BDV.DRAFT_TYPE, '1', '11410601', '11410701') AS ITEM_CD,  --科目号
	    CASE WHEN SUBSTR(BC.DIRECTION,2,4) IN('8610','8622','8710','8720','8740','6421','6429','8621','8623','8624','8625','8626','8629','8730','8770','8810','8870','8890','6572','6422','8831','8832','8840','8850','8860','2431','2432','2433','2434','2435','2436','2437','2438','2439','3075','3076','7251','7259','7491','7492','5143','5144','5145','5243','5244','7124','7125','6321','6322','6331','8750','8760','8820','5183','5184','5146','5245','5246','9011','9012','9013','9019','9020','9090','7850','7861','7862','7869','7712','7715','7716','9030','5622','2222','2642','2644','2664','2311','2312','2319','2320','2330','8060','7281','7284','7289','9051','9053','9059','7298','7121','7123','7350','8393','3542','3474','3931','3932','3933','3934','3939','5178','3471','3953','3472','3473','5248','3873','2461','2462','2469','2421','2422','2423','2429','5147','5247','2411','5141','5241','2412','2414','2451','2456','2459','2672','3951','3952','5137','5271','5149','5249') THEN 'Y' 
		     ELSE 'N'
	    END AS LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
	    DECODE(ACC.STATUS, '2', 0, BDV.DRAFT_AMOUNT) AS LOAN_ACCT_BAL,  --贷款余额
	    coalesce(BANKINFO.BRH_ZH_FULL_NAME,m2.BRH_ZH_FULL_NAME,CUST.ACC_BANK_NAME) AS LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
	    BCV.CUST_NAME AS LOAN_ACCT_NAME,  --贷款入账账户名称
	    BDV.AOA_ACCOUNT AS LOAN_ACCT_NUM,  --贷款入账账号
	    '9' AS LOAN_BUSINESS_TYP,  --贷款用途分类
	    'N' AS LOAN_BUY_INT,  --是否转入贷款
	    BDV.WARRANT_NO AS LOAN_FHZ_NUM,  --贷款分户账账号
	    TRIM(CASE WHEN BC.CLASSIFYRESULT = '01' THEN '1'
			 WHEN BC.CLASSIFYRESULT = '02' THEN '2'
			 WHEN BC.CLASSIFYRESULT = '03' THEN '3'
			 WHEN BC.CLASSIFYRESULT = '04' THEN '4'
			 WHEN BC.CLASSIFYRESULT = '05' THEN '5'
			 ELSE '1'
		END) AS LOAN_GRADE_CD,  --五级分类代码
	    /*CASE WHEN BDV.MIX_FLAG='1' OR  BDV.ORG_CD_RANGE='0' THEN BDV.DRAFT_NUMBER 
		     ELSE BDV.DRAFT_NUMBER||','||BDV.ORG_CD_RANGE
	    END AS LOAN_NUM,  --贷款编号*/
		BDV.WARRANT_NO AS LOAN_NUM,  --贷款编号   --by gjw 20240115
	    KJ4.DIQDAIMA AS LOAN_PURPOSE_AREA,  --贷款投向地区
	    BC.DIRECTION AS LOAN_PURPOSE_CD,  --贷款投向
	    'CHN' AS LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
	    'N' AS LOW_RISK_FLAG,  --是否低风险业务
	    --'3' AS MAIN_GUARANTY_TYP,  --主要担保方式
	    CASE WHEN SUBSTR(BC.VOUCHTYPE,0,3) IN ('040','050') THEN '0'
             WHEN BC.VOUCHTYPE LIKE '020%' THEN '1'
             WHEN SUBSTR(BC.VOUCHTYPE,0,3) IN ('010','070') THEN '2'
             WHEN BC.VOUCHTYPE ='005' THEN '3'
        END AS MAIN_GUARANTY_TYP,  --主要担保方式     by gjw 20231228 从信贷判断
	    BDV.MATURITY_DATE AS MATURITY_DT,  --原始到期日期   BTD.ACCT_DATE
	    BDV.MATURITY_DATE AS NEXT_INT_PAY_DT,  --下一付息日
	    BDV.MATURITY_DATE AS NEXT_REPRICING_DT,  --下一利率重定价日
	    CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE<=D_DATADATE THEN DATEDIFF(D_DATADATE , TO_DATE(BDV.PAYMENT_DATE))
			 ELSE 0
		END AS OD_DAYS,  --逾期天数
	    CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE <= D_DATADATE THEN 'Y'
	         ELSE 'N'
	    END AS OD_FLG,  --逾期标志
	    0 AS OD_INT,  --逾期利息
	    0 AS OD_INT_OBS,  --表外欠息
	    CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE <= D_DATADATE THEN BDV.DRAFT_AMOUNT
	         ELSE '0'
	    END AS OD_LOAN_ACCT_BAL,  --逾期贷款余额
	    BCV.BUSI_BRANCH_NO AS ORG_NUM,  --机构号
	    AI.OLDDUEBILLNO AS ORIG_ACCT_NO,  --原账号
	    'N' AS OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
	    BDV.ACCEPTOR_BANK_NAME AS PAY_ACCT_BANK,  --还款账号所属行名称
	    BC.ISFTZ AS PFTZ_LOAN_FLG,  --自贸区贷款标志
	    'N' AS POVERTY_ALLE,  --已脱贫人口贷款标志
	    '07' AS PPL_REPAY_FREQ,  --本金还款频率
	    'Z99' AS PRICING_BASE_TYPE,  --定价基础类型
	    CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE<=D_DATADATE THEN BDV.PAYMENT_DATE
			 ELSE ''
		END AS P_OD_DT,  --本金逾期日期
	    -- BDV.RATE AS REAL_INT_RAT,  --实际利率
	    CASE WHEN ACC.STATUS ='2' AND NVL(ACC.END_DT,TO_CHAR(ACC.UPDATE_TIME,'YYYYMMDD')) < BDV.MATURITY_DATE THEN cqc.rate * 100.00 ELSE BDV.RATE END AS REAL_INT_RAT,  --实际利率  20240906 贴现来源转贴现卖断的取卖出利率，卖出时的信贷员工
	    'N' AS RENEW_FLG,  --无还本续贷标志
	    'N' AS REPAY_FLG,  --借新还旧标志
	    '1' AS REPAY_TERM_NUM,  --还款总期数
	    '7' AS REPAY_TYP,  --还款方式_2
	   -- '期初一次性扣除利息' AS REPAY_TYP_DESC,  --还款方式说明
	    '承兑、贴现或者保证人到期付款' AS REPAY_TYP_DESC,  --还款方式说明   20240422 夏婷反馈改为这个 by qzj
	    'N' AS RESCHED_FLG,  --重组标志
	    'N' AS TAX_RELATED_FLG,  --银税合作贷款标志
	    'N' AS TRANSFERS_LOAN_FLG,  --转贷款标志
	    BC.PURPOSE AS USEOFUNDS,  --贷款用途
	    'N' AS YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志
		'D' AS GUARANTY_TYP,  --贷款担保方式	BY:YXY 20240419 默认担保方式为信用
	    CASE WHEN ACC.STATUS ='2' AND NVL(ACC.END_DT,TO_CHAR(ACC.UPDATE_TIME,'YYYYMMDD')) < BDV.MATURITY_DATE THEN 'Y' ELSE 'N' END AS LOAN_SELL_INT,  --转出标志
		datediff(TO_DATE(BDV.MATURITY_DATE),TO_DATE(BTD.ACCT_DATE)) AS ORIG_TERM,  --原始期限
        'D' ORIG_TERM_TYPE,  --原始期限类型
	    CASE
			WHEN BD.EXTENDTIMES <>0 THEN NVL(BC.TERMMONTH,CEIL(MONTHS_BETWEEN(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE))))
			ELSE CEIL(MONTHS_BETWEEN(TO_DATE(BD.MATURITY),TO_DATE(BD.PUTOUTDATE)))
		END AS ORIG_TERM_BC, --贷款原始期限-合同		
		CASE WHEN ACC.STATUS = '1' AND BDV.PAYMENT_DATE <= D_DATADATE THEN '2'
	         ELSE DECODE(ACC.STATUS, '2', '3', '1')
	    END AS ACCT_STS_FHZ,
		BC.SERIALNO AS ACCT_NUM_CQCS --业务合同号客户风险 by:yxy 20241120
	FROM OMI.BS_BUY_DETAILS_VIEW_HS BDV --买入明细视图
	INNER JOIN OMI.BS_BUY_CONTRACT_VIEW_HS BCV --买入批次视图
		   ON BDV.CONTRACT_ID = BCV.ID AND BCV.BEGNDT <= D_DATADATE AND BCV.OVERDT > D_DATADATE AND BCV.PARTID = V_PARTID
	INNER JOIN OMI.BS_BMS_TRADE_DRAFT_HS BTD --记账票据信息
		   ON BTD.CONTRACT_ID = BCV.ID AND BTD.DETAIL_ID = BDV.ID AND BTD.STATUS = '1' AND BTD.BEGNDT <= D_DATADATE AND BTD.OVERDT > D_DATADATE AND BTD.PARTID = V_PARTID
	INNER JOIN OMI.BS_BMS_ACCT_HISTORY_HS ACC --余额表
		   ON BDV.ID = ACC.DETAIL_ID AND ACC.BEGNDT <= D_DATADATE AND ACC.OVERDT > D_DATADATE AND ACC.PARTID = V_PARTID
	LEFT JOIN OMI.BS_MEM_BRH_INFO_HS BANKINFO --会员机构信息表
		   ON BANKINFO.UBANK_NO = BDV.AOA_BANK_ID AND BANKINFO.BEGNDT <= D_DATADATE AND BANKINFO.OVERDT > D_DATADATE AND BANKINFO.PARTID = V_PARTID
		   
	LEFT JOIN OMI.BS_CPES_QUOTE_CONTRACT_HS CQC 
		ON CQC.ID = ACC.SALE_CONTRACT_ID 		   
		AND CQC.BEGNDT <= D_DATADATE AND CQC.OVERDT > D_DATADATE AND CQC.PARTID = V_PARTID   --关联转贴现表获取转贴现卖出信息 佘小林 20240906
		
    LEFT JOIN OMI.BS_MEM_BRH_INFO_HS M2--SPE-20240605-0072 票据跨行贴现相关EAST逻辑优化
           ON M2.BRH_NO = BDV.AOA_BANK_ID AND M2.BRH_STATUS='ST01'
          AND M2.BEGNDT <= D_DATADATE
          AND M2.OVERDT > D_DATADATE
          AND M2.PARTID = V_PARTID
	LEFT JOIN OMI.CL_BUSINESS_DUEBILL_HS BD --业务借据信息
		   ON BDV.WARRANT_NO = BD.SERIALNO AND BD.BEGNDT <= D_DATADATE AND BD.OVERDT > D_DATADATE AND BD.PARTID = V_PARTID
	LEFT JOIN OMI.CL_BUSINESS_CONTRACT_HS BC --业务合同信息
		   ON BD.RELATIVESERIALNO2 = BC.SERIALNO AND BC.BEGNDT <= D_DATADATE AND BC.OVERDT > D_DATADATE AND BC.PARTID = V_PARTID
	LEFT JOIN OMI.I9_ECL_FINAL_RESULT_HS I9 --ECL最终计算结果表
		   ON BDV.WARRANT_NO = I9.ASSET_NO AND I9.BEGNDT <= D_DATADATE AND I9.OVERDT > D_DATADATE AND I9.PARTID = V_PARTID
	LEFT JOIN OMI.BS_BMS_CUSTOMER_INFO_HS CUST1 --
		   ON BCV.CUST_NO = CUST1.CUST_NO AND CUST1.BEGNDT <= D_DATADATE AND CUST1.OVERDT > D_DATADATE AND CUST1.PARTID = V_PARTID
    LEFT JOIN OMI.BS_BMS_CUST_ACCOUNT_INFO_HS CUST	--客户账户信息表
           ON CUST.ACCOUNT_NO = BDV.AOA_ACCOUNT AND CUST.BEGNDT <= D_DATADATE AND CUST.OVERDT > D_DATADATE AND CUST.PARTID = V_PARTID
	LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KJ4 --
		   ON (CASE WHEN CUST1.BLN_BRH_NO = '0090102' THEN '0090101' ELSE CUST1.BLN_BRH_NO END) = KJ4.JIGOUHAO AND KJ4.BEGNDT <= D_DATADATE AND KJ4.OVERDT > D_DATADATE AND KJ4.PARTID = V_PARTID
	LEFT JOIN M_DICT_REMAPPING_DL M --码值映射表
		   ON M.BUSINESS_TYPE = 'RSUM' AND M.DICT_CODE = 'CL-GREENLOANPURPOSE-GREEN_LOAN_TYPE' AND M.ORI_VALUES = BC.GREENLOANPURPOSE
	LEFT JOIN OMI.CL_ADVANCED_INFO_HS AI --
		   ON AI.DUEBILLNO = BD.SERIALNO AND AI.BEGNDT <= D_DATADATE AND AI.OVERDT > D_DATADATE AND AI.PARTID = V_PARTID
	WHERE BDV.BEGNDT <= D_DATADATE AND BDV.OVERDT > D_DATADATE AND BDV.PARTID = V_PARTID
	AND BCV.account_status = '03' --记账状态
     AND BDV.account_status = '03' --记账状态
     AND acc.START_DT <= TO_CHAR(D_DATADATE,'YYYYMMDD')
     /*AND (acc.STATUS = '1' or
         (acc.STATUS = '2' and to_char(acc.update_time, 'yyyyMMdd') >=
         substr(I_DATADATE, 1, 6) || '01'))  --by qzj 20240117 同生产east逻辑*/
	;
	
	
	
	
    V_STEP_ID   := V_STEP_ID + 1;
    V_STEP_FLAG := 0;
    V_DATA_COUNT := 0;
    V_STEP_DESC := '加工L_ACCT_LOAN表-第6段逻辑';
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

	INSERT INTO L_ACCT_LOAN (
	    ACCT_NUM,  --合同号
	    ACCT_STS,  --账户状态
	    ACCT_STS_DESC,  --账户状态说明
	    ACCT_TYP,  --账户类型
	    ACCU_INT_AMT,  --应计利息
	    ACCU_INT_FLG,  --计息标志
	    ACTUAL_MATURITY_DT,  --实际到期日期
	    BASE_INT_RAT,  --基准利率
	    BOOK_TYPE,  --账户种类
	    CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
	    CANCEL_FLG,  --核销标志
	    CIRCLE_LOAN_FLG,  --循环贷款标志
	    CONSECUTE_TERM_NUM,  --连续欠款期数
	    CUMULATE_TERM_NUM,  --累计欠款期数
	    CURRENT_TERM_NUM,  --当前还款期数
	    CURR_CD,  --币种
	    CUST_ID,  --客户号
	    DATA_DATE,  --数据日期
	    DATE_SOURCESD,  --数据来源
	    DEPARTMENTD,  --归属部门
	    DRAWDOWN_AMT,  --放款金额
	    DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
	    DRAWDOWN_DT,  --放款日期
	    DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
	    DRAWDOWN_TYPE,  --放款方式
	    DRAWDOWN_TYPE_DESC,
	    DRAWDOWN_TYPE_NEW,  --发放类型
	    EMP_ID,  --信贷员员工号
	    ENTRUST_PAY_AMT,  --受托支付金额
	    EXTENDTERM_FLG,  --展期标志
	    FINISH_DT,  --结清日期
	    FLOAT_TYPE,  --参考利率类型
	    FUND_USE_LOC_CD,  --贷款资金使用位置
	    GENERALIZE_LOAN_FLG,  --普惠型贷款标志
	    SP_PROV_AMT,  --一般准备
	    GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
	    GREEN_BALANCE,  --绿色信贷余额
	    GREEN_LOAN_FLG,  --绿色贷款标志
	    GUARANTY_TYP,  --贷款担保方式
	    INDEPENDENCE_PAY_AMT,  --自主支付金额
	    INDUST_STG_TYPE,  --战略新兴产业类型
	    INDUST_TRAN_FLG,  --工业企业技术改造升级标识
	    INTERNET_LOAN_FLG,  --互联网贷款标志
	    INT_RATE_TYP,  --利率类型
	    INT_REPAY_FREQ,  --利息还款频率
	    IS_FIRST_LOAN_TAG,  --是否首次贷款
	    IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
	    IS_REPAY_OPTIONS,  --是否内嵌提前还款权
	    ITEM_CD,  --科目号
	    I_OD_DT,  --利息逾期日期
	    LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
	    LOAN_ACCT_BAL,  --贷款余额
	    LOAN_ACCT_BANK,  --贷款入账账号所在银行名称
	    LOAN_ACCT_NAME,  --贷款入账账户名称
	    LOAN_ACCT_NUM,  --贷款入账账号
	    LOAN_BUSINESS_TYP,  --贷款用途分类
	    LOAN_BUY_INT,  --是否转入贷款
	    LOAN_FHZ_NUM,  --贷款分户账账号
	    LOAN_GRADE_CD,  --五级分类代码
	    LOAN_KIND_CD,  --贷款形式
	    LOAN_NUM,  --贷款编号
	    LOAN_NUM_OLD,  --原贷款编号
	    LOAN_PURPOSE_AREA,  --贷款投向地区
	    LOAN_PURPOSE_CD,  --贷款投向
	    LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
	    LOAN_RESCHED_DT,  --贷款重组日期
	    LOAN_SELL_INT,  --转出标志
	    LOW_RISK_FLAG,  --是否低风险业务
	    MAIN_GUARANTY_TYP,  --主要担保方式
	    MATURITY_DT,  --原始到期日期
	    NEXT_INT_PAY_DT,  --下一付息日
	    OD_DAYS,  --逾期天数
	    OD_FLG,  --逾期标志
	    OD_INT,  --逾期利息
	    OD_INT_OBS,  --表外欠息
	    OD_LOAN_ACCT_BAL,  --逾期贷款余额
	    ORG_NUM,  --机构号
	    ORIG_ACCT_NO,  --原账号
	    ORIG_TERM,  --原始期限
	    ORIG_TERM_TYPE,  --原始期限类型
	    OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
	    PAY_ACCT_BANK,  --还款账号所属行名称
	    PAY_ACCT_NUM,  --还款账号
	    PFTZ_LOAN_FLG,  --自贸区贷款标志
	    POVERTY_ALLE,  --已脱贫人口贷款标志
	    PPL_REPAY_FREQ,  --本金还款频率
	    P_OD_DT,  --本金逾期日期
	    REAL_INT_RAT,  --实际利率
	    RENEW_FLG,  --无还本续贷标志
	    REPAY_FLG,  --借新还旧标志
	    REPAY_TERM_NUM,  --还款总期数
	    REPAY_TYP,  --还款方式_2
	    REPAY_TYP_DESC,  --还款方式说明
	    RESCHED_FLG,  --重组标志
	    SECURITY_AMT,  --保证金金额
	    TAX_RELATED_FLG,  --银税合作贷款标志
	    TRANSFERS_LOAN_FLG,  --转贷款标志
	    UNDERTAK_GUAR_TYPE,  --是否创业担保贷款
	    USEOFUNDS,  --贷款用途
	    YANGTZE_RIVER_LOAN_FLG,   --长江经济带贷款标志
		NEXT_REPRICING_DT,   --下一利率重定价日
		MON_INT_INCOM,    --本年利息收入
		INT_ADJEST_AMT,  --利息调整
		GREEN_PURPOSE_CD,  --绿色融资投向
		PRICING_BASE_TYPE, --定价基础类型
		GUARANTY_TYP_JS, --贷款担保方式(金数)
        USEOFUNDS_js --贷款用途(金数)	 
	)
	WITH SUBQUERY_1 AS (
	SELECT SUM(AMT) as amt,T1.VCHNO
	FROM OMI.XL_DC_LOG_HS T1
	LEFT JOIN OMI.XL_COM_ITEM_HS T2 ON T1.ACC_HRT = T2.ACC_HRT AND T1.BANKID = T2.BANKID AND T2.BEGNDT >= D_DATADATE AND T2.OVERDT < D_DATADATE AND T2.PARTID = V_PARTID
	WHERE T1.BEGNDT >= D_DATADATE AND T1.OVERDT < D_DATADATE AND T1.PARTID = V_PARTID AND T2.UP_ACC_HRT = '5001' 
    AND T1.TX_DATE > (SELECT SUBSTR(PARVALUE,0,4)||'0101' FROM OMI.XL_CN_SYSPARM_HS WHERE PARCODE = '0001' AND STCD = 'E' AND PARNAME = 'sysdate' AND BEGNDT >= D_DATADATE AND OVERDT < D_DATADATE AND PARTID = V_PARTID) 
	GROUP BY T1.VCHNO),
	SUBQUERY_2 AS 
	(SELECT VCHNO,COUNT(*) AS CON FROM OMI.XL_LN_INT_STATUS_HS WHERE BEGNDT <= D_DATADATE AND OVERDT > D_DATADATE AND PARTID = V_PARTID AND STATUS='1' GROUP BY VCHNO)
	SELECT 
	    A.CONTNO AS ACCT_NUM,  --合同号
	    /*CASE WHEN A.LOAN_STS='1' THEN '1' --正常
			WHEN A.LOAN_STS IN ('2','3') THEN '2'   --逾期  SPE-20241106-0056
			WHEN A.LOAN_STS='*' THEN '3'  --结清  
			ELSE '9'
		END AS ACCT_STS,*/  --账户状态
		CASE WHEN A.LOAN_STS='1' THEN '1' --正常
			WHEN A.LOAN_STS IN ('2','3') THEN '2'   --逾期  SPE-20241106-0056
			WHEN A.LOAN_STS not in ('1','2','3','6','9') THEN '3'  --结清   SPE-20241211-0019
			ELSE '9'
		END AS ACCT_STS,  --账户状态
	    CASE --WHEN A.LOAN_STS='3' THEN '非应计'
			WHEN A.LOAN_STS='6' THEN '核销'
			WHEN A.LOAN_STS='9' THEN '撤销'
			--WHEN A.LOAN_STS='*' THEN '销户'
		END AS ACCT_STS_DESC,  --账户状态说明
	    CASE WHEN A.PRDT_NO LIKE '%A%' THEN '010299'
			WHEN A.PRDT_NO LIKE '%B%' THEN '0202'
		ELSE '019999' END AS ACCT_TYP,  --账户类型 
	    --F.SUM_INT AS ACCU_INT_AMT,  --应计利息    --********需要改写子查询
		nvl(ls1.CURRINT,'0') AS ACCU_INT_AMT,  --应计利息  --by 20240408 参照生产逻辑
		CASE WHEN CC.CON >0 THEN 'N' ELSE 'Y' END AS ACCU_INT_FLG,  --计息标志
	    CASE WHEN A.EXTENDDATE IS NULL THEN A.ENDDATE 
			ELSE A.EXTENDDATE
		END AS ACTUAL_MATURITY_DT,  --实际到期日期
	    --A.IRATE AS BASE_INT_RAT,  --基准利率
	    A.brate AS BASE_INT_RAT,  --基准利率  按老金数修改
	    '1' AS BOOK_TYPE,  --账户种类
	    DECODE(D.SCHOOL_LOAN_FLAG, '1', '1', '0', '2') AS CAMPUS_CONSU_LOAN_FLG,  --校园消费贷款标志
	    CASE WHEN B.AC_STS='6' THEN 'Y' 
			ELSE 'N'
		END AS CANCEL_FLG,  --核销标志
	    --DECODE(C.IS_CIC, 1, 'Y', 0, 'N') AS CIRCLE_LOAN_FLG,  --循环贷款标志 
	    'N' AS CIRCLE_LOAN_FLG,  --循环贷款标志   by gjw 20240321 与业务董鑫确认，小微贷没有循环贷
	    A.NOW_OVERDUE_PERIOD AS CONSECUTE_TERM_NUM,  --连续欠款期数
	    --A.MAX_OVERDUE_PERIOD AS CUMULATE_TERM_NUM,  --累计欠款期数
		A.OVERDUE_PERIOD AS CUMULATE_TERM_NUM,  --累计欠款期数   --by 20240408 参照生产逻辑
	    A.NOW_PERIOD AS CURRENT_TERM_NUM,  --当前还款期数
	    A.CURR AS CURR_CD,  --币种
	    --NVL(G.OUTCIFID,G.CERTNO) AS CUST_ID,  --客户号
	    NVL(G.OUTCIFID,A.CIFID) AS CUST_ID,  --客户号
	    TO_CHAR(D_DATADATE, 'YYYYMMDD') AS DATA_DATE,  --数据日期
	    'XL_6' AS DATE_SOURCESD,  --数据来源
	    'xwdk' AS DEPARTMENTD,  --归属部门
	    A.BUS_SUM AS DRAWDOWN_AMT,  --放款金额
	    A.IRATE AS DRAWDOWN_BASE_INT_RAT,  --放款时点基准利率
	    A.BEGINDATE AS DRAWDOWN_DT,  --放款日期
	    A.ARATE AS DRAWDOWN_REAL_INT_RAT,  --放款时点实际利率
	    /*CASE WHEN A.PAY_TYPE='1' THEN 'A'
			WHEN A.PAY_TYPE='2' THEN 'B'
        ELSE 'Z'
		END AS DRAWDOWN_TYPE,*/  --放款方式
		CASE WHEN A.PAY_TYPE='1' AND A.OCCURTYPE NOT IN ('03','06','07') THEN 'A'
			 WHEN A.PAY_TYPE='2' AND A.OCCURTYPE NOT IN ('03','06','07') THEN 'B'            
		ELSE 'Z'
		END AS DRAWDOWN_TYPE,  --放款方式   --SPE-20241106-0056
		CASE WHEN A.OCCURTYPE='03' THEN '借新还旧' --SPE-20241106-0056
	         WHEN A.OCCURTYPE='06' THEN '续贷'	   --SPE-20241106-0056
	         WHEN A.OCCURTYPE='07' THEN '重组'   --SPE-20241106-0056
	         WHEN A.PAY_TYPE IS NULL THEN '历史早期贷款'  -- ADD BY SPE-20240808-0009
	    END AS DRAWDOWN_TYPE_DESC,
	    CASE WHEN A.OCCURTYPE='02' THEN '展期'
		     WHEN A.OCCURTYPE='03' THEN '借新还旧'
			 WHEN A.OCCURTYPE='04' THEN '收回再贷'
			 WHEN A.OCCURTYPE='05' THEN '变更批复'
			 WHEN A.OCCURTYPE='06' THEN '续贷'
			ELSE NULL
		END AS DRAWDOWN_TYPE_NEW,  --发放类型
	    A.MANAGE_OPERID AS EMP_ID,  --信贷员员工号
	    CASE WHEN A.PAY_TYPE='2' THEN E.BUS_SUM
			ELSE 0
		END AS ENTRUST_PAY_AMT,  --受托支付金额
	    CASE
	        WHEN EXT_NUM > 0 THEN 'Y'
	        ELSE 'N'
	    END AS EXTENDTERM_FLG,  --展期标志
	    NVL(A.FINISHDATE,AV.OCCUR_DATE) AS FINISH_DT,  --结清日期
	    CASE
	        WHEN A.BASE_RATE_TYPE = '0' THEN 'A'
	        ELSE 'C'
	    END AS FLOAT_TYPE,  --参考利率类型
	    'I' AS FUND_USE_LOC_CD,  --贷款资金使用位置
	    'Y' AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
	   -- DECODE(D.PH_FARM_LOAN_FLAG, '0', 'N', '1', 'Y') AS GENERALIZE_LOAN_FLG,  --普惠型贷款标志
	    B.LOSS_BAL AS SP_PROV_AMT,  --一般准备
	    'N' AS GOV_BONDMORG_FLG,  --是否地方政府专项债券配套融资
	    CASE
	        WHEN D.IS_GRELOAN_CBRC = '1' THEN dd.bus_bal  --应该是dd表不存在
	        ELSE 0
			END
	     AS GREEN_BALANCE,  --绿色信贷余额
	    DECODE(D.IS_GRELOAN_CBRC, 1, 'Y', 0, 'N') AS GREEN_LOAN_FLG,  --绿色贷款标志
	    CASE WHEN A.GUAR_TYPE = '10' AND (A.PRDT_NO LIKE 'A01%' OR A.PRDT_NO LIKE 'B04%') THEN 'B01'
			WHEN A.GUAR_TYPE = '10' THEN 'B99'
			WHEN A.GUAR_TYPE ='20' THEN  'A' 
			WHEN A.GUAR_TYPE ='30' THEN  'C99' 
			WHEN A.GUAR_TYPE ='50' THEN 'D' 
		END AS GUARANTY_TYP,  --贷款担保方式
	    CASE
	        WHEN A.PAY_TYPE = '1' THEN A.BUS_SUM
	        ELSE 0
	    END AS INDEPENDENCE_PAY_AMT,  --自主支付金额
	    D.OC_PRCP AS INDUST_STG_TYPE,  --战略新兴产业类型
		--by:yxy 20240517 该字段上游有两个来源 OC_PRCP/战略性新兴产业分类，用1-7区分；loan_target_field1/是否战略性新兴产业(0否1是)；暂时使用OC_PRCP字段
	    --'2' AS INDUST_TRAN_FLG,  --工业企业技术改造升级标识
		CASE
			WHEN D.LOAN_TARGET_FIELD = '1' THEN '1'
			ELSE '2'
		END AS INDUST_TRAN_FLG,  --工业企业技术改造升级标识
		--by:yxy 20240517 参照大数据平台修改取数逻辑
	    'N' AS INTERNET_LOAN_FLG,  --互联网贷款标志
	    CASE WHEN A.FLOAT_MOD='1' THEN 'F'
			WHEN A.RATE_FTYPE IN ('1','2') THEN 'L5'
			WHEN A.RATE_FTYPE='0' AND A.TERM_TYPE ='D' THEN 'L0'
			WHEN A.RATE_FTYPE='0' AND A.TERM_TYPE ='M' THEN 'L2'
	    ELSE 'L9'
		END AS INT_RATE_TYP,  --利率类型
	    CASE WHEN A.REPAY_FREQ='1' THEN '03' 
			WHEN A.REPAY_FREQ='3' THEN '04' 
			WHEN A.REPAY_FREQ='4' THEN '05' 
			WHEN A.REPAY_FREQ='5' THEN '06'
            ELSE '07' 
		END AS INT_REPAY_FREQ,  --利息还款频率
	    DECODE(D.FIRST_LOAN_OWN, 1, 1, 0, 2) AS IS_FIRST_LOAN_TAG,  --是否首次贷款
	    CASE WHEN FIRST_LOAN_OWN='1' THEN '1' 
			WHEN FIRST_LOAN_OWN='0' THEN '2'
		END AS IS_FIRST_REPORT_LOAN_TAG,  --是否报送机构首次贷款
	    'N' AS IS_REPAY_OPTIONS,  --是否内嵌提前还款权
	    (SELECT CC.ACC_NO FROM OMI.XL_LN_PARM_HS AA, OMI.XL_DC_ACC_REL_HS B,OMI.XL_COM_ITEM_HS CC WHERE 
	    AA.BEGNDT <= D_DATADATE AND AA.OVERDT > D_DATADATE AND AA.PARTID = V_PARTID AND
	    B.BEGNDT <= D_DATADATE AND B.OVERDT > D_DATADATE AND B.PARTID = V_PARTID AND
	    CC.BEGNDT <= D_DATADATE AND CC.OVERDT > D_DATADATE AND CC.PARTID = V_PARTID AND
	    AA.DC_CODE = B.DC_CODE AND AA.BANKID = B.BANKID AND B.ACC_HRT = CC.ACC_HRT AND B.DC_TYPE = '1' AND AA.PRDT_NO = A.CORE_PRDT_NO) AS ITEM_CD,  --科目号	    
		--A.LO_DATE AS I_OD_DT,  --利息逾期日期   --by 20240408 参照生产逻辑
		LS3.REPENDDATE AS I_OD_DT,  --利息逾期日期   --by 20240408 参照生产逻辑
	    --'N' AS LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
		CASE
			WHEN D.LOAN_TARGET_FIELD2 = '1' THEN '1'
			ELSE '2'
		END AS LOANS_TO_CULTURE_FLAG,  --贷款投向文化及相关产业标志
		--by:yxy 20240517 参照大数据平台修改取数逻辑
	    --A.BUS_BAL AS LOAN_ACCT_BAL,  --贷款余额
	    CASE WHEN A.LOAN_STS IN ('4','6') THEN 0 ELSE A.BUS_BAL END AS LOAN_ACCT_BAL,  --贷款余额
	    NVL(KK.JGMC,KK4.JGMC) AS LOAN_ACCT_BANK,  --贷款入账账号所在银行名称   --BY 20240709
	    --nvl(A.DEPACCNA,A.REPPRIACNA) AS LOAN_ACCT_NAME,  --贷款入账账户名称
	    --NVL(A.DEPACC_NO,A.REPPRIAC_NO) AS LOAN_ACCT_NUM,  --贷款入账 账号
		nvl(G.CLINAME,A.DEPACCNA) as DKRZHM,  --贷款入账户名   20231214  董鑫确认   贷款入账户名为空时取客户名称
		CASE WHEN A.DEPACC_NO IS NULL AND A.vchno NOT LIKE 'CON%' THEN A.REPPRIAC_NO    --当贷款入账账号为空 并且借据号为老个贷系统借据号时   取还款账号作为 贷款入账账号  20231214  by 宫长珑 霍秀杰
             ELSE A.DEPACC_NO 
        END  AS LOAN_ACCT_NUM,  --贷款入账 账号  --by 20240408 参照生产逻辑
	    --A.LOAN_PURPOSE AS USEOFUNDS,  --贷款用途分类
		CASE WHEN A.PURPOSE = '18' THEN '9' ELSE '5' END AS LOAN_BUSINESS_TYP,  --贷款用途分类
		'N' AS LOAN_BUY_INT,  --是否转入贷款  
	    A.CONTNO AS LOAN_FHZ_NUM,  --贷款分户账账号
	    TRIM(CASE WHEN A.CLSFIVE ='0102' THEN '1'
			 WHEN A.CLSFIVE = '0202' THEN '2'
			 WHEN A.CLSFIVE = '0302' THEN '3'
			 WHEN A.CLSFIVE='0402' THEN '4' 
			 WHEN A.CLSFIVE='0502' THEN '5' 
			 END) AS LOAN_GRADE_CD,  --五级分类代码
	    TRIM(CASE WHEN A.OCCURTYPE='01' THEN '1'
			WHEN A.OCCURTYPE='03' THEN '3'
			WHEN A.OCCURTYPE='04' THEN '2'
			WHEN A.OCCURTYPE='06' THEN '99'
			WHEN A.OCCURTYPE='02' THEN '91'
		END) AS LOAN_KIND_CD,  --贷款形式
	    CASE WHEN A.DATA_SOURCE = 'LSXD' THEN substr(A.VCHNO,1,14)||'1' ELSE A.VCHNO END AS LOAN_NUM,  --贷款编号  BY 20230911 原个贷借据处理，李钰莹提供
	    -- A.ORG_VCHNO AS LOAN_NUM_OLD,  --原贷款编号
	    avr.ORG_VCHNO AS LOAN_NUM_OLD,  --原贷款编号  20241023 HXJ 修改上笔借据取数逻辑  SPE-20241025-0015
	    --A.INDUSTRYTYPE AS LOAN_PURPOSE_AREA,  --贷款投向地区  --by gjw 20231120
	    NVL(CASE WHEN G.CSTYPE='C01' THEN EB.REGIONCODE
                 WHEN G.CSTYPE='A01' THEN IB.REGIONCODE 
			 END,coalesce(KK.DIQDAIMA,KK4.DIQDAIMA,KK5.DIQDAIMA)) AS LOAN_PURPOSE_AREA,  --贷款投向地区  by 20240709
	    --A.INDUSTRYTYPE AS LOAN_PURPOSE_CD,  --贷款投向
	    D.INDUSTRYTYPE AS LOAN_PURPOSE_CD,  --贷款投向  --by gjw 20240228
	    'CHN' AS LOAN_PURPOSE_COUNTRY_NEW,  --贷款投向国别
	    CASE
	       WHEN A.OCCURTYPE = '02' THEN A.BEGNDT
	       ELSE ''
	    END AS LOAN_RESCHED_DT,  --贷款重组日期  --*********  需要表别名
	    'N' AS LOAN_SELL_INT,  --转出标志
	    'N' AS LOW_RISK_FLAG,  --是否低风险业务
	    CASE WHEN A.GUAR_TYPE='10'THEN '1'
			WHEN A.GUAR_TYPE='20'THEN '0'
			WHEN A.GUAR_TYPE='30'THEN '2'
			WHEN A.GUAR_TYPE='50'THEN '3'
		END AS MAIN_GUARANTY_TYP,  --主要担保方式
	    A.ENDDATE AS MATURITY_DT,  --原始到期日期
	    A.NEXT_PAY_INT_DATE AS NEXT_INT_PAY_DT,  --下一付息日
	    NVL(GREATEST(BAL_OVERDAY,INT_OVERDAY),0) AS OD_DAYS,  --逾期天数
	    CASE
	        WHEN A.LO_DATE IS NULL THEN 'N'
	        ELSE 'Y'
	    END AS OD_FLG,  --逾期标志
	    --B.IN_OVER_INTST + B.OUT_OVER_INTST AS OD_INT,  --逾期利息
		nvl(B.IN_LO_INTST,0) AS OD_INT,  --逾期利息
	    B.OUT_LO_INTST AS OD_INT_OBS,  --表外欠息
	    --A.OVER_BAL AS OD_LOAN_ACCT_BAL,  --逾期贷款余额
		B.OVER_BAL AS OD_LOAN_ACCT_BAL,  --逾期贷款余额
	    --A.MANAGE_INSTCODE AS ORG_NUM,  --机构号
	    --A.INSTCODE AS ORG_NUM,  --机构号   BY GJW 20231012  MANAGE_INSTCODE字段有很多后四位为0000
	    A.BAL_INSTCODE AS ORG_NUM,  --机构号   BY GJW 20240227
	    A.ORG_VCHNO AS ORIG_ACCT_NO,  --原账号
	    A.TERM AS ORIG_TERM,  --原始期限
	    A.TERM_TYPE AS ORIG_TERM_TYPE,  --原始期限类型
	    'N' AS OVERSEA_ANN_LOAN_FLAG,  --境外并购贷款标志
	    nvl(KK1.JGMC,kk5.JGMC) AS PAY_ACCT_BANK,  --还款账号所属行名称   --BY 20240709
	    A.REPPRIAC_NO AS PAY_ACCT_NUM,  --还款账号
	    'N' AS PFTZ_LOAN_FLG,  --自贸区贷款标志
	    CASE WHEN D.JZFP_CUST_TYPE='01' THEN 'Y' ELSE 'N' END AS POVERTY_ALLE,  --已脱贫人口贷款标志
	    CASE WHEN A.REPAY_FREQ='1' THEN '03' 
			WHEN A.REPAY_FREQ='3' THEN '04' 
			WHEN A.REPAY_FREQ='4' THEN '05' 
			WHEN A.REPAY_FREQ='5' THEN '06' 
		END AS PPL_REPAY_FREQ,  --本金还款频率	    
		--A.LO_DATE AS P_OD_DT,  --本金逾期日期
		LS2.REPENDDATE AS P_OD_DT,  --本金逾期日期
	    A.ARATE AS REAL_INT_RAT,  --实际利率
	    CASE
	        WHEN A.OCCURTYPE='06' THEN 'Y'
	        ELSE 'N'
	    END AS RENEW_FLG,  --无还本续贷标志
	    CASE
	        WHEN A.OCCURTYPE='03' THEN 'Y'
	        ELSE 'N'
	    END AS REPAY_FLG,  --借新还旧标志
	    A.PERIOD_ALL AS REPAY_TERM_NUM,  --还款总期数
	    CASE WHEN A.REPAY_TYPE='1' THEN '7'
			WHEN A.REPAY_TYPE='2' THEN '7'
			WHEN A.REPAY_TYPE='3' THEN '5'
			WHEN A.REPAY_TYPE='4' THEN '6'
			WHEN A.REPAY_TYPE='5' THEN '7'
			WHEN A.REPAY_TYPE='6' THEN '7'
			WHEN A.REPAY_TYPE='7' THEN '7'
			WHEN A.REPAY_TYPE='8' THEN '7'
		END AS REPAY_TYP,  --还款方式_2
	    CASE WHEN A.REPAY_TYPE='1' THEN '等额本息'
			WHEN A.REPAY_TYPE='2' THEN '等额本金'
		    WHEN A.REPAY_TYPE='5' THEN '净息还款'
			WHEN A.REPAY_TYPE='6' THEN '自定义还款计划'
			WHEN A.REPAY_TYPE='7' THEN '弹性还款'
			WHEN A.REPAY_TYPE='8' THEN '气球贷'
		END AS REPAY_TYP_DESC,  --还款方式说明
	    CASE WHEN A.OCCURTYPE='02' THEN 'Y'   
			ELSE 'N'
		END AS RESCHED_FLG,  --重组标志
	    0 AS SECURITY_AMT,  --保证金金额
	    'N' AS TAX_RELATED_FLG,  --银税合作贷款标志
	    DECODE(A.USE_TRANSFER_FUNDS,'1','Y','0','N') AS TRANSFERS_LOAN_FLG,  --转贷款标志
	   --- DECODE(D.STARTUPGUAUANTEE_FLAG,'0','N','1','Y') AS UNDERTAK_GUAR_TYPE,  --是否创业担保贷款
	   -- 按照张静祎提供逻辑进行修改，金数专项二批使用   by wzm 2024-5-9 11:32:44
	   CASE WHEN D.STARTUPGUAUANTEE_FLAG ='1' THEN 'Z' /*ELSE ''*/ END AS UNDERTAK_GUAR_TYPE,  --是否创业担保贷款
	    --A.LOAN_PURPOSE AS USEOFUNDS,  --贷款用途
		XCS.DDVALUE AS USEOFUNDS,     --贷款用途  by gjw 20230914
	    'N' AS YANGTZE_RIVER_LOAN_FLG,  --长江经济带贷款标志
		/*case when a.FLOAT_MOD='1' then null  
             when a.FLOAT_MOD='2' and a.RATE_FTYPE='5' and to_char(D_DATADATE,'yyyymmdd')> to_char(D_DATADATE,'yyyy')||substr(A.BEGNDT,5,8) then to_char(D_DATADATE,'yyyy')+1||substr(begindate,5,8) 
             when a.FLOAT_MOD='2' and a.RATE_FTYPE='5' and to_char(D_DATADATE,'yyyymmdd')< to_char(D_DATADATE,'yyyy')||substr(A.BEGNDT,5,8) then to_char(D_DATADATE,'yyyy')||substr(begindate,5,8) 
             when a.FLOAT_MOD='2' and a.RATE_FTYPE='6' and to_char(D_DATADATE,'yyyymmdd')> to_char(D_DATADATE,'yyyy')||substr(A.BEGNDT,5,8) then to_char(D_DATADATE,'yyyy')+1||'0101'
        else null end AS NEXT_REPRICING_DT, */
	    NVL(B.next_rateadj_date,A.enddate) AS NEXT_REPRICING_DT, --按老金数修改
		BB.AMT AS MON_INT_INCOM, --来源表数据缺少卡的条件限制，此字段数据可能偏大
		0 AS INT_ADJEST_AMT,  --利息调整
		M1.NEW_VALUES AS GREEN_PURPOSE_CD,  --绿色融资投向(银监描述)
		trim(case when A.BASE_RATE_TYPE='0' then 'C'--贷款市场报价利率  0 贷款市场报价利率(LPR); 1 伦敦同业拆借利率(Libor; 2 香港同业拆借利率(Hibor); 3 人行基准利率
	         WHEN A.BASE_RATE_TYPE='1' then 'A0201'
		     WHEN A.BASE_RATE_TYPE='2' then 'A0202'
		     WHEN A.BASE_RATE_TYPE='3' then 'B02' --按老金数修改
		else ''
	    END) PRICING_BASE_TYPE --定价基础类型
		,XL.DBFS AS guaranty_typ_js
		,case when A.PURPOSE = "101" then "购进原材料"
              when A.PURPOSE = "102" then "购进设备"
	          when A.PURPOSE = "103" then "加盟"
	          when A.PURPOSE = "104" then "开店"
	          when A.PURPOSE = "105" then "流动资金"
	          when A.PURPOSE = "106" then "营运周转"
	          when A.PURPOSE = "107" then "扩大经营"
	          when A.PURPOSE = "108" then "更换设备"
	          when A.PURPOSE = "109" then "进货"
	          when A.PURPOSE = "111" then "支付租金"
	          when A.PURPOSE = "113" then "囤货备货"
	          when A.PURPOSE = "20" then "小微经营"
	          when A.PURPOSE = "19" then "惠农通贷款"
	          when A.PURPOSE = "10" then "经营贷款"
	          when A.PURPOSE = "16" then "创业贷款"
	          when A.PURPOSE = "21" then "借新还旧"
	          when A.PURPOSE = "22" then "第四中支扶贫专项贷款"
	          when A.PURPOSE = "23" then "房抵经营"
	          when A.PURPOSE = "27" then "续贷"
	          when A.PURPOSE = "28" then "置换贷款"
	          when A.PURPOSE = "18" then "其他"
              END AS USEOFUNDS_js --贷款用途_金数 
	FROM OMI.XL_AC_BUSINESSVCH_HS A --借款借据表
	LEFT JOIN OMI.XL_LN_MST_HS B --贷款分支主文件表
	ON A.VCHNO = B.VCHNO AND B.BEGNDT <= D_DATADATE AND B.OVERDT > D_DATADATE AND B.PARTID = V_PARTID
	LEFT JOIN OMI.XL_AC_BUSINESSCONT_HS C --借款合同表
	ON A.CONTNO = C.CONTNO AND C.BEGNDT <= D_DATADATE AND C.OVERDT > D_DATADATE AND C.PARTID = V_PARTID
	LEFT JOIN OMI.XL_BUS_DATA_REPORT_HS D --业务数据报送表
	ON A.APPLYNO = D.APPLYNO AND D.BEGNDT <= D_DATADATE AND D.OVERDT > D_DATADATE AND D.PARTID = V_PARTID
	--by:yxy 20240513 将原来的 C.APPLYNO = D.APPLYNO 修改为 A.APPLYNO = D.APPLYNO 从合同表修改为借据表
--20241023 HXJ end  add by SPE-20241025-0015
		left  join  (select a.APPLYNO,concat_ws('|',COLLECT_LIST(CASE WHEN substr(a.vchno,0,1)='1' then  substr(a.vchno,1,14)||'1' ELSE a.vchno end )) as org_vchno 
			from  omi.xl_app_vch_rel_hs A where   
			a.partid = v_partid
			AND a.begndt <= D_DATADATE
			AND a.overdt > D_DATADATE   --add by  20241025 拼接上笔信贷借据号  --add by SPE-20241025-0015
		group by a.applyno
		) avr
		on c.applyno=avr.applyno 
--20241023 HXJ end  add by SPE-20241025-0015	
	LEFT JOIN (SELECT sum(a.bus_bal) AS bus_bal,A.VCHNO FROM OMI.XL_AC_BUSINESSVCH_HS A WHERE A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID GROUP BY A.VCHNO)dd
	ON DD.VCHNO = A.VCHNO
	LEFT JOIN (SELECT E.APPLYNO,SUM(E.BUS_SUM) AS BUS_SUM FROM OMI.XL_AC_TEPAYMENT_HS E --
	            WHERE E.BEGNDT <= D_DATADATE AND E.OVERDT > D_DATADATE AND E.PARTID = V_PARTID
	            GROUP BY E.APPLYNO
	 ) E
	ON C.APPLYNO = E.APPLYNO
	LEFT JOIN (SELECT *,ROW_NUMBER() OVER(PARTITION BY VCHNO ORDER BY CURRCNT ASC) AS RM  
			FROM  OMI.XL_LN_SCHEDULE_HS 
			WHERE 	REPENDDATE>TO_CHAR(D_DATADATE,'YYYYMMDD') -- wky 20240414 转成用8位日期
					AND PARTID = V_PARTID
				   	AND BEGNDT <= D_DATADATE
				   	AND OVERDT > D_DATADATE)  LS1 --还款计划表
	 ON LS1.VCHNO=A.VCHNO
	 AND LS1.RM='1'
    --start  add by SPE-20240626-0039	 
     LEFT JOIN (SELECT MIN(REPENDDATE) AS REPENDDATE ,VCHNO 
			FROM OMI.XL_LN_SCHEDULE_HS 
			WHERE LO_IND='1' AND REPAYSTATE='0' AND  CURRAMT-PAYSUM>0
			  AND PARTID = V_PARTID
			  AND BEGNDT <= D_DATADATE
			  AND OVERDT > D_DATADATE GROUP BY VCHNO)	LS2--20240624 欠本日期  修改取数逻辑  SPE-20240626-0039 
			   ON LS2.VCHNO=A.VCHNO
      LEFT JOIN (SELECT MIN(REPENDDATE) AS REPENDDATE ,VCHNO 
		   FROM OMI.XL_LN_SCHEDULE_HS 
		  WHERE LO_IND='1' AND REPAYSTATE='0' AND  CURRINT-PAYINT+OVER_INT-PAY_OVER_INT>0
		    AND PARTID = V_PARTID
		    AND BEGNDT <= D_DATADATE
			AND OVERDT > D_DATADATE GROUP BY VCHNO)	LS3--20240624 欠息日期  修改取数逻辑  SPE-20240626-0039 
			 ON LS3.VCHNO=A.VCHNO
	LEFT JOIN OMI.XL_AC_VERIFI_INFO_HS AV  --核销信息表
    ON A.VCHNO = AV.VCHNO
    AND AV.BEGNDT <= D_DATADATE AND AV.OVERDT > D_DATADATE AND AV.PARTID = V_PARTID 
	LEFT JOIN OMI.XL_CUST_INFO_HS G --
	ON A.CIFID = G.CIFID AND G.BEGNDT <= D_DATADATE AND G.OVERDT > D_DATADATE AND G.PARTID = V_PARTID
	LEFT JOIN OMI.XL_ENT_BASE_HS EB
    ON G.CIFID = EB.CIFID AND EB.BEGNDT <= D_DATADATE AND EB.OVERDT > D_DATADATE AND EB.PARTID = V_PARTID
    LEFT JOIN OMI.XL_IND_BASE_HS IB
    ON G.CIFID = IB.CIFID AND IB.BEGNDT <= D_DATADATE AND IB.OVERDT > D_DATADATE AND IB.PARTID = V_PARTID
	LEFT JOIN (SELECT KK.KEHUZHAO,CASE WHEN JG.JIGOUHAO ='0010001' THEN '大连银行股份有限公司' ELSE JG.JIGOUZWM END AS JGMC,DIQDAIMA 
                 FROM  OMI.KN_KDPA_KEHUZH_HS KK
                 LEFT JOIN OMI.KN_KBRP_JGCSHU_HS JG
                   ON KK.KAIHJIGO = JG.JIGOUHAO
                  AND JG.PARTID = V_PARTID AND JG.BEGNDT <= D_DATADATE AND JG.OVERDT > D_DATADATE
                WHERE KK.BEGNDT <= D_DATADATE AND KK.OVERDT > D_DATADATE AND KK.PARTID = V_PARTID) KK
    ON KK.KEHUZHAO = CASE WHEN A.DEPACC_NO IS NULL AND A.vchno NOT LIKE 'CON%' THEN A.REPPRIAC_NO    --当贷款入账账号为空 并且借据号为老个贷系统借据号时   取还款账号作为 贷款入账账号  20231214  by 宫长珑 霍秀杰
                          ELSE A.DEPACC_NO 
                     END
					 
	LEFT JOIN (SELECT DISTINCT AIM.ACNO,CASE WHEN JG.JIGOUHAO ='0010001' THEN '大连银行股份有限公司' ELSE JG.JIGOUZWM END AS JGMC,DIQDAIMA 
                 FROM URDM.ACNO_INSTCODE_MAPPING AIM
                 LEFT JOIN OMI.KN_KBRP_JGCSHU_HS JG
                   ON AIM.INSTCODE = JG.JIGOUHAO
                  AND JG.PARTID = V_PARTID AND JG.BEGNDT <= D_DATADATE AND JG.OVERDT > D_DATADATE
				WHERE AIM.ACNO_TYPE='2'  --账号类别：1、还款账号，2.入账账号
                ) KK4
    ON KK4.ACNO = CASE WHEN A.DEPACC_NO IS NULL AND A.vchno NOT LIKE 'CON%' THEN A.REPPRIAC_NO    --当贷款入账账号为空 并且借据号为老个贷系统借据号时   取还款账号作为 贷款入账账号  20231214  by 宫长珑 霍秀杰
                          ELSE A.DEPACC_NO 
                     END	--by 20240709 
	LEFT JOIN (SELECT KK.KEHUZHAO,CASE WHEN JG.JIGOUHAO ='0010001' THEN '大连银行股份有限公司' ELSE JG.JIGOUZWM END AS JGMC 
                 FROM  OMI.KN_KDPA_KEHUZH_HS KK
                 LEFT JOIN OMI.KN_KBRP_JGCSHU_HS JG
                   ON KK.KAIHJIGO = JG.JIGOUHAO
                  AND JG.PARTID = V_PARTID AND JG.BEGNDT <= D_DATADATE AND JG.OVERDT > D_DATADATE
                WHERE KK.BEGNDT <= D_DATADATE AND KK.OVERDT > D_DATADATE AND KK.PARTID = V_PARTID) KK1
    ON KK1.KEHUZHAO = A.REPPRIAC_NO
	LEFT JOIN (SELECT DISTINCT AIM.ACNO,CASE WHEN JG.JIGOUHAO ='0010001' THEN '大连银行股份有限公司' ELSE JG.JIGOUZWM END AS JGMC,DIQDAIMA  
                 FROM  URDM.ACNO_INSTCODE_MAPPING AIM
                 LEFT JOIN OMI.KN_KBRP_JGCSHU_HS JG
                   ON AIM.INSTCODE = JG.JIGOUHAO
                  AND JG.PARTID = V_PARTID AND JG.BEGNDT <= D_DATADATE AND JG.OVERDT > D_DATADATE
				WHERE AIM.ACNO_TYPE='1'  --账号类别：1、还款账号，2.入账账号
				) KK5
    ON KK5.ACNO = A.REPPRIAC_NO    --by 20240709
    LEFT JOIN OMI.KN_KDPA_KEHUZH_HS KKK1  --核心客户账号表 取入账账号所属行名称
	ON KKK1.KEHUZHAO=A.DEPACC_NO
	   AND KKK1.PARTID = V_PARTID
	   AND KKK1.BEGNDT <= D_DATADATE
	   AND KKK1.OVERDT > D_DATADATE	   
    LEFT JOIN OMI.KN_KBRP_JGCSHU_HS KKJ --机构参数表 取贷款投向地区
	ON KKK1.KAIHJIGO=KKJ.JIGOUHAO
	   AND KKJ.PARTID = V_PARTID
	   AND KKJ.BEGNDT <= D_DATADATE
	   AND KKJ.OVERDT > D_DATADATE
    LEFT JOIN OMI.XL_CN_SOPTLIST_HS   XCS  --使用码表  
	ON  XCS.DCCODE='loanPurpose' 
	AND XCS.DDCODE= A.PURPOSE
	AND XCS.PARTID =V_PARTID
	AND XCS.BEGNDT<=D_DATADATE
	AND XCS.OVERDT>D_DATADATE
	LEFT JOIN SUBQUERY_1 BB   
	ON BB.VCHNO = A.VCHNO
	LEFT JOIN SUBQUERY_2 CC   
	ON CC.VCHNO = A.VCHNO
	LEFT JOIN M_DICT_REMAPPING_DL M1 --小微贷码值映射表
           ON D.GRE_INDUSTRYTYPE_CBRC = REPLACE(M1.ORI_VALUES,'%','')
		  AND M1.BUSINESS_TYPE = 'RSUM'
		  AND M1.DICT_CODE = 'XL-GRE_INDUSTRYTYPE_CBRC-GREEN_PURPOSE_CD'
	LEFT JOIN 	  
    (SELECT 
    DISTINCT 
    case when t1.PURPOSE = "101" then "购进原材料"
     when t1.PURPOSE = "102" then "购进设备"
	 when t1.PURPOSE = "103" then "加盟"
	 when t1.PURPOSE = "104" then "开店"
	 when t1.PURPOSE = "105" then "流动资金"
	 when t1.PURPOSE = "106" then "营运周转"
	 when t1.PURPOSE = "107" then "扩大经营"
	 when t1.PURPOSE = "108" then "更换设备"
	 when t1.PURPOSE = "109" then "进货"
	 when t1.PURPOSE = "111" then "支付租金"
	 when t1.PURPOSE = "113" then "囤货备货"
	 when t1.PURPOSE = "20" then "小微经营"
	 when t1.PURPOSE = "19" then "惠农通贷款"
	 when t1.PURPOSE = "10" then "经营贷款"
	 when t1.PURPOSE = "16" then "创业贷款"
	 when t1.PURPOSE = "21" then "借新还旧"
	 when t1.PURPOSE = "22" then "第四中支扶贫专项贷款"
	 when t1.PURPOSE = "23" then "房抵经营"
	 when t1.PURPOSE = "27" then "续贷"
	 when t1.PURPOSE = "28" then "置换贷款"
	 when t1.PURPOSE = "18" then "其他"
     END AS PURPOSE
	 --,case when substr(t1.VCHNO,1,3)<>"CON" then substr(t1.VCHNO,0,14)||"1" else t1.VCHNO END AS LOAN_NUM 
	 ,t1.VCHNO
	 ,t2.DBFS AS DBFS
      FROM  omi.xl_AC_BUSINESSVCH_hs T1
	  LEFT JOIN 
	  ( SELECT
	distinct
	t1.VCHNO,t1.cifid,t2.guar_type,t2.oth_guar_type,t5.col_no,
	case when t2.guar_type ="20" and  t2.oth_guar_type is null then "A" --纯质押
	when t2.guar_type ="50" AND t2.oth_guar_type is null then  "D" --纯信用
	when t2.guar_type ="10" AND t2.oth_guar_type is not NULL and substr(t5.col_no,1,4) IN ("0301","0306")  then  "E01" --E01：其中：含房地产抵押的组合担保 
	when  t2.guar_type is not NULL  AND  t2.oth_guar_type is not null AND substr(nvl(t5.col_no,0),1,4) NOT IN ("0301","0306")  then  "E"--E：组合担保
	when t2.guar_type ="10"  and  t2.oth_guar_type is null and  substr(t5.col_no,1,4) IN ("0301","0306")   then "B01" --B01：房地产抵押贷款 
	when t2.guar_type ="10"  and  t2.oth_guar_type is null and  substr(nvl(t5.col_no,0),1,4) NOT IN ("0301","0306")   then "B99" -- B99：其他抵押贷款  
	when t2.guar_type ="30"  and  t2.oth_guar_type is null and t6.GUARFORM="30"   then  "C01"--C：保证贷款  C01：联保贷款  C99：其他保证贷款 
	when t2.guar_type ="30"  and  t2.oth_guar_type is null and t6.GUARFORM!="30"   then  "C99"--C：保证贷款  C01：联保贷款  C99：其他保证贷款 
	when substr(t5.col_no,1,4) IN ("0301","0306") then "E01"
	else "E" end   as dbfs
	FROM
	  omi.xl_ac_businessvch_hs t1
	LEFT JOIN omi.xl_ac_businesscont_hs t2
	ON t1.contno=t2.contno
	and t2.begndt <=D_DATADATE
	and t2.overdt > D_DATADATE
	and t2.partid = TO_CHAR(D_DATADATE,'YYYYMM')
	LEFT JOIN omi.xl_bus_gcc_relative_hs t3
	ON t2.contno=t3.contno --and t3.status="20"   --担保合同状态已生效
	and t3.applyno = t2.applyno
	and t3.begndt <=D_DATADATE
	and t3.overdt > D_DATADATE
	and t3.partid = TO_CHAR(D_DATADATE,'YYYYMM')
	LEFT JOIN omi.xl_ac_guarcont_hs t6
	ON t6.CONTNO=t3.gccontno
	and t6.begndt <=D_DATADATE
	and t6.overdt > D_DATADATE
	and t6.partid = TO_CHAR(D_DATADATE,'YYYYMM')
	LEFT JOIN
		omi.xl_bus_affrim_collateral_hs t4
	ON t4.applyno =t3.applyno  and  t4.cifid IS NOT null    --t4.applyno =t3.applyno
	and t4.begndt <=D_DATADATE
	and t4.overdt > D_DATADATE
	and t4.partid = TO_CHAR(D_DATADATE,'YYYYMM')
	LEFT JOIN omi.xl_guar_base_hs t5
	ON t4.guar_no =t5.guar_no
	and t5.begndt <=D_DATADATE
	and t5.overdt > D_DATADATE
	and t5.partid = TO_CHAR(D_DATADATE,'YYYYMM')
	WHERE	
	t1.begndt <=D_DATADATE
	and t1.overdt > D_DATADATE
	and t1.partid = TO_CHAR(D_DATADATE,'YYYYMM')) t2
      ON t1.VCHNO = t2.VCHNO
      AND t1.cifid = t2.cifid
   LEFT JOIN  (SELECT VCHNO,cifid,guar_type,oth_guar_type  --微贷金数取担保方式
       FROM (
       SELECT
       t1.VCHNO,t1.cifid,t2.guar_type,t2.oth_guar_type,substr(t5.col_no,1,4) col_no
       FROM
           omi.xl_ac_businessvch_hs t1
       LEFT JOIN
           omi.xl_ac_businesscont_hs t2
       ON
           t1.contno=t2.contno
       and t2.begndt <=D_DATADATE 
       and t2.overdt > D_DATADATE 
       and t2.partid = TO_CHAR(D_DATADATE,'YYYYMM')
       LEFT JOIN
           omi.xl_bus_gcc_relative_hs t3
       ON
           t2.contno=t3.contno --and t3.status="20"   --担保合同状态已生效
       AND  t3.applyno =t2.applyno
       and t3.begndt <=D_DATADATE 
       and t3.overdt > D_DATADATE 
       and t3.partid = TO_CHAR(D_DATADATE,'YYYYMM')
       LEFT JOIN
           omi.xl_ac_guarcont_hs t6
       ON
           t6.CONTNO=t3.gccontno
       and t6.begndt <=D_DATADATE 
       and t6.overdt > D_DATADATE 
       and t6.partid = TO_CHAR(D_DATADATE,'YYYYMM')
       LEFT JOIN
           omi.xl_bus_affrim_collateral_hs t4
       ON
           t4.applyno =t3.applyno  and  t4.cifid IS NOT null    --t4.applyno =t3.applyno
       and t4.begndt <=D_DATADATE 
       and t4.overdt > D_DATADATE 
       and t4.partid = TO_CHAR(D_DATADATE,'YYYYMM')
       LEFT JOIN
           omi.xl_guar_base_hs t5
       ON
           t4.guar_no =t5.guar_no
       and t5.begndt <=D_DATADATE 
       and t5.overdt > D_DATADATE 
       and t5.partid = TO_CHAR(D_DATADATE,'YYYYMM')
       WHERE
           
       t1.begndt <=D_DATADATE 
       and t1.overdt > D_DATADATE 
       and t1.partid = TO_CHAR(D_DATADATE,'YYYYMM')
       GROUP BY t1.VCHNO,t1.cifid,t2.guar_type,t2.oth_guar_type,substr(t5.col_no,1,4)
       ) 
       GROUP BY VCHNO,cifid,guar_type,oth_guar_type
       HAVING count(*) > 1 ) t3
             on t2.VCHNO=t3.VCHNO 
             and t2.cifid=t3.cifid 
             and t2.guar_type=t3.guar_type 
             and nvl(t2.oth_guar_type,"99999")=nvl(t3.oth_guar_type,"99999")   
              WHERE  T1.BEGNDT <= D_DATADATE 
              AND T1.OVERDT > D_DATADATE 
              AND  T1.PARTID = TO_CHAR(D_DATADATE,'YYYYMM')
              AND  (t3.VCHNO IS NULL OR (t3.VCHNO IS NOT NULL AND (substr(t2.col_no,1,4) IN ("0301","0306") OR substr(t2.col_no,1,4)="0404")))
              ) XL
	 ON A.VCHNO =  XL.VCHNO
	WHERE A.BEGNDT <= D_DATADATE AND A.OVERDT > D_DATADATE AND A.PARTID = V_PARTID
	AND A.BUS_SUM > 0
	AND A.VCH_STS<>'20'; 
	   
  
--是否报送机构首次贷款
MERGE INTO (SELECT * FROM L_ACCT_LOAN WHERE DATA_DATE = I_DATADATE) A
USING (SELECT L.LOAN_NUM,DATE_SOURCESD,'Y' AS IS_FIRST_REPORT_LOAN_TAG 
  FROM (
  SELECT L.LOAN_NUM,DATE_SOURCESD,ROW_NUMBER() OVER(PARTITION BY CUST_ID ORDER BY DRAWDOWN_DT,LOAN_NUM) RN
    FROM L_ACCT_LOAN L
   WHERE L.DATA_DATE = I_DATADATE) L
   WHERE L.RN = 1) B
ON (A.LOAN_NUM = B.LOAN_NUM AND A.date_sourcesd = B.date_sourcesd) WHEN MATCHED THEN
  UPDATE SET A.IS_FIRST_REPORT_LOAN_TAG = B.IS_FIRST_REPORT_LOAN_TAG;
  COMMIT;    
	
    V_DATA_COUNT := SQL%ROWCOUNT;
    V_STEP_FLAG := 1;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);
    
    V_STEP_DESC := V_PROCEDURE||'逻辑处理完成';
    V_DATA_COUNT := 0;
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);

EXCEPTION
  WHEN OTHERS THEN
    V_ERRORCODE := SQLCODE;
    V_ERRORDESC := SUBSTR(SQLERRM, 1, 280);
    V_STEP_DESC := V_ERRORCODE || V_ERRORDESC;
    V_STEP_FLAG := -1;
    V_DATA_COUNT := 0;
    --记录异常信息
    SP_ETL_LOG(I_DATADATE, V_SCHEMA, V_PROCEDURE, V_STEP_ID, V_STEP_DESC, V_STEP_FLAG, V_DATA_COUNT, SYSTIMESTAMP);
    RAISE;
END sp_l_acct_loan
/
!set plsqlUseSlash false"
"use urdm;
!set plsqlUseSlash true
