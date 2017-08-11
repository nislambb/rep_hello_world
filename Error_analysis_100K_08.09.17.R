# comment1 to check github
# second change

setwd('D:/Nasrul/my_projects/vehicle_history/error_analysis_100K/vh2_nearest_plus_adj_factor_change')


install.packages("RODBC")
library(RODBC)

install.packages("plyr")  
library(plyr)

install.packages("dplyr")
library(dplyr)                 #detach(package:dplyr,unload=TRUE)

install.packages("lubridate")
library(lubridate)

library(parallel)

install.packages("doSNOW")
library(doSNOW)

install.packages("doParallel")
library(doParallel)

library(hydroGOF)

library(reshape)
library(reshape2)

install.packages('Rmisc')
library(Rmisc)

#https://stackoverflow.com/questions/33556385/cant-manage-to-install-the-xlsx-package-on-windows-10-64bit
# install.packages("xlsxjars", INSTALL_opts = "--no-multiarch")
# library(xlsxjars)
# install.packages('rJava')
# library(rJava)
# install.packages("xlsx")
# library(xlsx)

install.packages('openxlsx') 
library(openxlsx)

install.packages('ggplot2')
library(ggplot2)

#newAmazon = 34.202.209.181 (Name:MSSQL-DEV-01) oldAmazon = 54.153.82.213
new_db_handle <- odbcDriverConnect('driver={SQL Server};server=MSSQL-DEV-01;uid=AnalyticsUser;pwd=HappyCoding!.')
#old_db_handle <- odbcDriverConnect('driver={SQL Server};server=54.153.82.213;uid=nislam;pwd=')

## required look-up tables ============================================================================
group_table <- sqlQuery(new_db_handle,"select * from [VehicleHistory].[dbo].[vh_activity_code_group]")
category_table <- sqlQuery(new_db_handle,"select * from [VehicleHistory].[dbo].[vh_group_to_category]")
impact_table <- sqlQuery(new_db_handle,"select * from [VehicleHistory].[dbo].[vh_impact_pct]")
range_table <- sqlQuery(new_db_handle,"select * from [VehicleHistory].[dbo].[vh_impact_dollar_range]")
class_table <- sqlQuery(new_db_handle,"select * from [VehicleHistory].[dbo].[vh_impact_segment_factor]")
cross_product_table <- sqlQuery(new_db_handle,"select * from [VehicleHistory].[dbo].[vh_cross_product_transformation]")



#### Performance evaluation 100K vin ==================================================================================================================================================================


qry_price_100K_vin <- "select  Distinct mii.vin, mii.sale_date, mii.ownerCount,rsg.CLASS_CODE class_code,vd.model_year model_yr 
		                           --,abs(hsd.sale_price - mii.sale_price) dif_sale_pr  -- for testing only
                               --wholesale mileage adjusted prices
                              ,(case when  abs(hm.Avg) > 0.5*(hh.avg + hh.AVG_CALC) then round(0.5*(hh.avg + hh.AVG_CALC)/25,0)*25 else hh.avg + hh.AVG_CALC + hm.Avg end ) ws_avg_pr
                              ,(case when abs(hm.Xclean) > 0.5*hh.xcl then round(0.5*hh.xcl/25,0)*25 else hh.xcl + hm.Xclean end) ws_xcl_pr
                              ,(case when abs(hm.Clean) > 0.5*hh.cln then round(0.5*hh.cln/25,0)*25 else hh.cln + hm.Clean end) ws_cln_pr
                              ,(case when  abs(hm.Rough) > 0.5*(hh.rgh + hh.RGH_CALC) then round(0.5*(hh.rgh + hh.RGH_CALC)/25,0)*25 else hh.rgh + hh.RGH_CALC + hm.Rough end ) ws_rgh_pr
                              
                              --retail mileage adjusted prices
                              ,(case when  abs(hm.Avg) > 0.5*(hh.ravg + hh.ravg_CALC) then round(0.5*(hh.ravg + hh.ravg_CALC)/25,0)*25 else hh.ravg + hh.ravg_CALC + hm.Avg end ) r_avg_pr
                              ,(case when abs(hm.Xclean) > 0.5*hh.rxcl then round(0.5*hh.rxcl/25,0)*25 else hh.rxcl + hm.Xclean end) r_xcl_pr
                              ,(case when abs(hm.Clean) > 0.5*hh.rcln then round(0.5*hh.rcln/25,0)*25 else hh.rcln + hm.Clean end) r_cln_pr
                              ,(case when  abs(hm.Rough) > 0.5*(hh.rrgh + hh.rrgh_CALC) then round(0.5*(hh.rrgh + hh.rrgh_CALC)/25,0)*25 else hh.rrgh + hh.rrgh_CALC + hm.Rough end ) r_rgh_pr
                              
                              --trade_in mileage adjusted prices
                              ,(case when  abs(hm.Avg) > 0.5*(hh.avgtrade + hh.TAVG_CALC) then round(0.5*(hh.avgtrade + hh.TAVG_CALC)/5,0)*5 else hh.avgtrade + hh.TAVG_CALC + hm.Avg end ) t_avg_pr 
                              ,0 t_xcl_pr
                              ,(case when abs(hm.Clean) > 0.5*hh.cleantrade then round(0.5*hh.cleantrade/5,0)*5 else hh.cleantrade + hm.Clean end) t_cln_pr 
                              ,(case when  abs(hm.Rough) > 0.5*(hh.roughtrade + hh.TRGH_CALC) then round(0.5*(hh.roughtrade + hh.TRGH_CALC)/5,0)*5 else hh.roughtrade + hh.TRGH_CALC + hm.Rough end ) t_rgh_pr 
                              
                              -- aditional info
                              --,DateAdd(Month, DateDiff(Month,0, hsd.[sale_date]), 0) as sale_date_01  -- to join with mileage adj tables. sql assumes base (0) as 1900-01-01
                              ,mii.sale_price,  mii.mileage
                              --,mii.bb_ma_price -- for testing purpose 
                              ,hsd.vehicle_id  --,hsd.full_vin_flag
                              ,vd.reporting_segment_code seg_code
                              ,mii.seg_group
                              ,(case when mii.seg_group like '%car%' then 1 else 0 end) IsCar
                              ,(case when mii.seg_group like '%non_lux%' then 0 else 1 end) IsLuxury 
                              --,vd.reporting_segment_name
                              --,vd.reporting_segment_type
                              ,vd.mileage_cat
                              --,(mii.bb_ma_price - (hh.avg + hh.AVG_CALC+hm.avg)) diff_from_hao  -- for testing only 
                              
                      from    [Editorial].[dbo].[autocheck_modeling_input_id] mii  -- for sample vins with auto check history
                              inner join  [Editorial].[auc].[historical_survey_data] hsd on hsd.vin = mii.vin and hsd.sale_date = mii.sale_date -- vehicle_id
                              inner join [BBDyn].[dbo].[History] as hh  -- for unadjusted bb prices 
                                      on hsd.vehicle_id = hh.[vehicle_id_nb] and  [BBDyn].[dbo].[fn_GetDate] (mii.sale_date, 'D', 'u') = hh.actualdate and hh.datafreq = 'D'
                              inner join Editorial.dbo.vehicle_data vd on vd.vehicle_id = hsd.vehicle_id -- for model_yr and segment. Other tables (hsd,hh, etc are not reliable)
                              inner join [Autobahn].[dbo].[REPORTING_SEGMENT_CATEGORY] rsg on rsg.CODE = vd.reporting_segment_code  -- for 1 char class_code
                              inner join BBDyn.[dbo].[History_mileage] hm   -- for mileage adjustment 
                                      on hm.Mileage_cat = vd.mileage_cat and hm.Full_Year = vd.model_year and mii.mileage >= hm.Range_begin and mii.mileage <= hm.Range_end and [BBDyn].[dbo].[fn_GetDate] (mii.sale_date, 'D', 'u') = hm.Historical_date 
                              
                      where   mii.sale_date >= '2016-09-01' and mii.sale_date <= '2017-01-24' 
                              and hsd.match_result <> 'mult' 
                              --and hsd.[mileage]  >100 and hsd.[mileage] <999999
                              and hm.DataFreq = 'D'   "

input_vehicle_all_vars_100K <- sqlQuery(new_db_handle,qry_price_100K_vin) 
input_vehicle_all_vars_100K$sale_date <- as.Date(input_vehicle_all_vars_100K$sale_date,format = "%Y-%m-%d") #,format = "%Y-%m-%d"
input_vehicle_all_vars_100K <- unique(input_vehicle_all_vars_100K)

input_vehicle_100K <- input_vehicle_all_vars_100K[,c(1:17)]  # only the necessary input prices to be compatible with function 
input_vehicle_100K$age_veh <- ifelse(is.na(input_vehicle_100K$sale_date),year(Sys.Date())-input_vehicle_100K$model_yr,year(input_vehicle_100K$sale_date)-input_vehicle_100K$model_yr)
input_vehicle_100K$age_veh <- ifelse(input_vehicle_100K$age_veh>20,20,input_vehicle_100K$age_veh)
input_vehicle_100K <- unique(input_vehicle_100K)

qry_hist_100K_vin <- "select distinct vins.vin, avh.activity_code, avh.date activity_date  

                      from  (select mii.vin from [Editorial].[dbo].[autocheck_modeling_input_id] mii where  mii.sale_date >= '2016-09-01' and mii.sale_date <= '2017-01-24'
                      --union
                      --select distinct sv.vin from [VehicleHistory].[dbo].[vh_sample_vins] sv 
                      ) vins  
                      left join [Editorial].[dbo].[AUTOCHECK_VEHICLE] av on av.vin = vins.vin
                      left join [Editorial].[dbo].[AUTOCHECK_VEHICLE_HISTORY] avh on avh.ac_vehicle_id = av.id "


input_history_100K <- sqlQuery(new_db_handle,qry_hist_100K_vin) 
input_history_100K$activity_date <- as.Date(input_history_100K$activity_date,format = "%Y-%m-%d")

close(new_db_handle)

source('D:/Nasrul/my_projects/vehicle_history/test_10K_plus_error/source_v13.R')

start <- Sys.time()

impact_event_100K  <- fn_event_impact_calc(input_vehicle_100K,input_history_100K, group_table ,category_table, impact_table, range_table)

time_taken <- Sys.time() - start
time_taken

impact_event_100K$hit_range <- ifelse(impact_event_100K$preDollarImpact != impact_event_100K$range_adj_dollar, 1, 0)
impact_event_100K <- unique(impact_event_100K)

#prepare for paralle processing
numCore <- detectCores()
cl <- makeCluster(numCore-5)
registerDoParallel(cl)

#make the necessary objects available to all cores 
clusterExport(cl,c('impact_event_100K','fn_semi_final_ws_avg_ha_calc'),envir = environment())

#pass all required functions and libraries to all cores 
invisible(clusterEvalQ(cl, 'source_v13.R'))
invisible(clusterEvalQ(cl, library(plyr)))
invisible(clusterEvalQ(cl, library(dplyr)))
#invisible(clusterEvalQ(cl, library(lubridate)))

start <- Sys.time()
#we may not get the same number of row as in input_price table as data may have duplicate entries (AUTOCHECK_VEHICLE has duplicates vins)
semi_fin_ws_avg_impact_100K = ddply(impact_event_100K, .(vin, sale_date, ws_avg_pr), function(x) fn_semi_final_ws_avg_ha_calc(x) , .parallel = T)

time_taken <- Sys.time() - start
time_taken

#last column does not have a good name
colnames(semi_fin_ws_avg_impact_100K)[ncol(semi_fin_ws_avg_impact_100K)] <- 'impact_before_seg_adj'

stopCluster(cl)

impact_product_100K <- fn_join_required_tables(semi_fin_ws_avg_impact_100K, input_vehicle_100K, class_table, cross_product_table)
impact_product_100K <- unique(impact_product_100K)

price_adj_factor <- fn_adj_factor_calc(impact_product_100K)

fin_ha_price_100K <-  fn_ha_price_calc(price_adj_factor)

#write.csv(fin_ha_price_100K,'final_ha_price_100K_v10.csv',row.names = F)


### End of History adjusted price estimate =================================================================================





##data preparation for performance evaluation 


in_out_combine <- merge(fin_ha_price_100K,input_vehicle_all_vars_100K) # will join based ion all common columns 
in_out_combine <- unique(in_out_combine)

in_out_combine <- fn_nearest_condition(in_out_combine)

in_out_combine$impact_direction <- ifelse(in_out_combine$seg_adj_impact_ws_avg_r25 ==0, 0,
                                          ifelse(in_out_combine$seg_adj_impact_ws_avg_r25 > 0, 1, -1))
in_out_combine$impact_direction <- as.factor(in_out_combine$impact_direction)

in_out_combine$cur_pred_dir <- ifelse(in_out_combine$cur_ws_avg_ref == in_out_combine$sale_price, 'bb_same',
                                          ifelse(in_out_combine$cur_ws_avg_ref > in_out_combine$sale_price, 'bb_over_pred', 'bb_under_pred'))

in_out_combine$ha_pred_dir <- ifelse(in_out_combine$ha_ws_avg_ref == in_out_combine$sale_price, 'ha_same',
                                      ifelse(in_out_combine$ha_ws_avg_ref > in_out_combine$sale_price, 'ha_over_pred', 'ha_under_pred'))


# in_out_combine$cur_pred_dir <- as.factor(in_out_combine$cur_pred_dir)

in_out_combine$sale_price_bin <- cut(in_out_combine$sale_price,c(0,500,1000,2000,5000,10000,20000,30000,40000,50000,60000,80000,max(in_out_combine$sale_price)+1))
in_out_combine$age_bin <- cut(in_out_combine$age_veh, c(-1,2,5,8,10,15,20))  # changed to -1 as seems not inclusive

in_out_combine$dev_ha <- (in_out_combine$ha_ws_avg_ref - in_out_combine$sale_price)/in_out_combine$sale_price
in_out_combine$dev_cur <- (in_out_combine$cur_ws_avg_ref - in_out_combine$sale_price)/in_out_combine$sale_price

in_out_combine$ha_better_or_eq <- ifelse(abs(in_out_combine$dev_cur)>=abs(in_out_combine$dev_ha),1,0)

in_out_combine$ha_performance <- ifelse(abs(in_out_combine$dev_cur)>abs(in_out_combine$dev_ha),"ha_better",
                                         ifelse(abs(in_out_combine$dev_cur)==abs(in_out_combine$dev_ha),"ha_same","ha_worse"))

in_out_combine$ha_too_harsh <- ifelse(in_out_combine$cur_pred_dir=='bb_over_pred' & in_out_combine$impact_direction==-1 
                                      & in_out_combine$ha_better_or_eq == 0, "too_harsh", "ok" )

### if want to create subset ==================================================================

#in_out_combine <- subset(in_out_combine, abs(dev_cur) < 1.5)


# numCore <- detectCores()
# cl <- makeCluster(numCore-5)
# registerDoParallel(cl)
# 
# #make the necessary objects available to all cores 
# clusterExport(cl,c('impact_event_100K'),envir = environment())
# 
# #pass all required functions and libraries to all cores 
# #invisible(clusterEvalQ(cl, 'source_v13.R'))
# invisible(clusterEvalQ(cl, library(plyr)))
# invisible(clusterEvalQ(cl, library(dplyr)))
vin_override_count_100K <- ddply(impact_event_100K, .(vin,sale_date,ws_avg_pr), summarise, count_override = sum(override))
write.csv(vin_override_count_100K,'vin_override_count_100K_08.04.17.csv', row.names = F)
# stopCluster(cl)

vin_override_count_100K <- read.csv('vin_override_count_100K_08.04.17.csv')
vin_override_count_100K$sale_date <- as.Date(vin_override_count_100K$sale_date, format = '%Y-%m-%d')

collect_sale_price <- select(in_out_combine, c(vin, sale_date, ws_avg_pr, sale_price))

vins_with_sale_price_orc <- merge(vin_override_count_100K,collect_sale_price)


vin_outliers <- subset(vins_with_sale_price_orc, sale_price <= 500 & count_override == 0) 
vin_not_outliers <- subset (vins_with_sale_price_orc, !(vin  %in% vin_outliers$vin) )

in_out_combine <- subset (in_out_combine, (vin  %in% vin_not_outliers$vin) )

# write.csv(in_out_combine,'in_out_combine_before_floor_check.csv', row.names = F)


source('D:/Nasrul/my_projects/vehicle_history/test_10K_plus_error/source_v13.R')

# Error on full data 
overall_error_100K_v13 <- fn_overall_error_nearest(in_out_combine)
# overall_error_100K_nearest <- fn_overall_error_nearest(in_out_combine)

error_age_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'age_veh')
error_seg_code_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'seg_code')
error_seg_group_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'seg_group')
error_impact_dir_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'impact_direction')
error_price_bin_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'sale_price_bin')
error_cur_pred_dir_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'cur_pred_dir')
error_car_100K_v13 <- fn_group_error_analysis_nearest(in_out_combine, 'IsCar')

error_age_impact_dir_100K_v13 <- fn_group2_error_analysis_nearest(in_out_combine, 'age_veh','impact_direction')
error_seg_impact_dir_100K_v13 <- fn_group2_error_analysis_nearest(in_out_combine, 'seg_code','impact_direction')



# event wise error analysis 
event_combine <- merge(in_out_combine,impact_event_100K)
event_combine <- unique(event_combine) # just for safety
error_incident_100K_v13 <- fn_group_error_analysis_nearest(event_combine, 'description')
error_incident_impact_dir_100K_v13 <- fn_group2_error_analysis_nearest(event_combine, 'description','impact_direction')

# event_set <- event_combine %>% arrange(vin, sale_date,ws_avg_pr,ws_avg_ha_pr,sale_price, seg_code,age_veh,IsCar,impact_direction,ha_better_or_eq,dev_ha,dev_cur,seg_adj_impact_ws_avg,description) %>% group_by(vin, sale_date,ws_avg_pr,ws_avg_ha_pr,sale_price, seg_code,age_veh,IsCar,impact_direction,ha_better_or_eq,dev_ha,dev_cur) %>% mutate(events = paste(description,collapse=','))
# event_set <- event_set %>%  rowwise() %>% distinct(vin, sale_date,ws_avg_pr,ws_avg_ha_pr,sale_price, seg_code,age_veh,IsCar,impact_direction,ha_better_or_eq,dev_ha,dev_cur,seg_adj_impact_ws_avg,events)

event_set <- event_combine %>% arrange(vin, sale_date,ws_avg_pr,cur_ws_avg_ref,ha_ws_avg_ref,sale_price, seg_code,age_veh,IsCar,impact_direction,ha_better_or_eq,dev_ha,dev_cur,seg_adj_impact_ws_avg_r25,description) %>% group_by(vin, sale_date,ws_avg_pr,cur_ws_avg_ref,ha_ws_avg_ref,sale_price, seg_code,age_veh,IsCar,impact_direction,ha_better_or_eq,dev_ha,dev_cur) %>% mutate(events = paste(description,collapse=','))
event_set <- event_set %>%  rowwise() %>% distinct(vin, sale_date,ws_avg_pr,cur_ws_avg_ref,ha_ws_avg_ref,sale_price, seg_code,age_veh,IsCar,impact_direction,ha_better_or_eq,dev_ha,dev_cur,seg_adj_impact_ws_avg_r25,events)

error_event_set_100K_v13 <- fn_group_error_analysis_nearest(event_set, 'events')

wb <- createWorkbook()
addWorksheet(wb,'overall_error')
addWorksheet(wb,'age_group_error')
addWorksheet(wb,'seg_code_error')
addWorksheet(wb,'seg_group_error')
addWorksheet(wb,'impact_dir_error')
addWorksheet(wb,'priceBin_group_error')
addWorksheet(wb,'cur_pred_error')
addWorksheet(wb,'car_group_error')


addWorksheet(wb,'incident_group_error')
addWorksheet(wb,'eventSet_group_error')
addWorksheet(wb,'age_impact_dir_error')
addWorksheet(wb,'seg_impact_dir_error')
addWorksheet(wb,'incident_impact_dir_error')

writeData (wb, 'overall_error', overall_error_100K_v13, rowNames = F  )
writeData (wb, 'age_group_error', error_age_100K_v13, rowNames = F  )
writeData (wb, 'seg_code_error', error_seg_code_100K_v13, rowNames = F  )
writeData (wb, 'seg_group_error', error_seg_group_100K_v13, rowNames = F  )
writeData (wb, 'impact_dir_error', error_impact_dir_100K_v13, rowNames = F  )
writeData (wb, 'priceBin_group_error', error_price_bin_100K_v13, rowNames = F  )
writeData (wb, 'cur_pred_error', error_cur_pred_dir_100K_v13, rowNames = F  )
writeData (wb, 'car_group_error', error_car_100K_v13, rowNames = F  )


writeData (wb, 'incident_group_error', error_incident_100K_v13, rowNames = F  )
writeData (wb, 'eventSet_group_error', error_event_set_100K_v13, rowNames = F  )
writeData (wb, 'age_impact_dir_error', error_age_impact_dir_100K_v13, rowNames = F  )
writeData (wb, 'seg_impact_dir_error', error_seg_impact_dir_100K_v13, rowNames = F  )
writeData (wb, 'incident_impact_dir_error', error_incident_impact_dir_100K_v13, rowNames = F  )
#writeData (wb, 'age_group_error', error_age_100K_v10, rowNames = F  )
#Sys.setenv("R_ZIPCMD" = "path/to/zip.exe")

saveWorkbook(wb,'error_analysis_100K_remOutlier_v13_08.09.17.xlsx')



## Error on set where ha is worse  ============================

in_out_combine_worse <- subset(in_out_combine, ha_better_or_eq == 0 ) #& cur_pred_dir == 'bb_over_pred'
error_worse_age_100K_v10 <- fn_group_error_analysis(in_out_combine_worse, 'age_veh')

#the worse
ggplot(in_out_combine_worse %>%count(age_veh)%>%mutate(pct=n/sum(n)),aes(age_veh,n)) + geom_bar(stat='identity') + geom_text(aes(label=paste0(sprintf('%1.1f',pct*100),'%')))
#overall  
ggplot(in_out_combine %>%count(age_veh)%>%mutate(pct=n/sum(n)),aes(age_veh,n)) + geom_bar(stat='identity') + geom_text(aes(label=paste0(sprintf('%1.1f',pct*100),'%')))

#compare with overall distribution
worse <- in_out_combine_worse %>%count(age_veh)%>%mutate(pct_worse=n/sum(n))
colnames(worse) <- c('age_veh','count_worse','pct_worse')
overall <- in_out_combine %>%count(age_veh)%>%mutate(pct_overall=n/sum(n))
colnames(overall) <- c('age_veh','count_overall','pct_overall')
join_worse_overall <- merge(overall,worse)
join_worse_overall <- melt(data=join_worse_overall, id.vars = 'age_veh', measure.vars = c('pct_overall', 'pct_worse') )                              

#p1 with data label
ggplot(join_worse_overall,aes(age_veh,value,color=variable)) + geom_line(stat = 'identity', position='dodge') + geom_text(aes(label=paste0(sprintf('%1.1f',value*100),'%')) ) + labs(title = 'Distribution of worse vs overall', y = 'relative frequency')

p1 <- ggplot(join_worse_overall,aes(age_veh,value,color=variable)) + geom_line(stat = 'identity', position='dodge')  + labs(title = 'Distribution of worse vs overall', y = 'relative frequency')
p2 <- ggplot(in_out_combine %>% count(age_bin, cur_pred_dir), aes(x=age_bin,y=n,fill=cur_pred_dir))+ geom_bar(stat='identity') + geom_text(aes(label=n), position=position_stack(vjust = 0.5)) 
p3 <- ggplot(in_out_combine %>% count(age_bin, ha_pred_dir), aes(x=age_bin,y=n,fill=ha_pred_dir))+ geom_bar(stat='identity') + geom_text(aes(label=n), position=position_stack(vjust = 0.5)) 
p4 <- ggplot(in_out_combine %>% count(age_bin, ha_too_harsh), aes(x=age_bin,y=n,fill=ha_too_harsh))+ geom_bar(stat='identity') + geom_text(aes(label=n), position=position_stack(vjust = 0.5)) 

multiplot(p1,p2, p3, p4, cols=2)

harshly_treated <- subset(in_out_combine, ha_too_harsh == 'too_harsh')

harsh_count <- harshly_treated %>% count(age_veh)
colnames(harsh_count) <- c('age_veh','count_harsh')

cur_overpred_sample <- subset(in_out_combine, cur_pred_dir == 'bb_over_pred')
overpred_count <- cur_overpred_sample %>% count(age_veh)
colnames(overpred_count) <- c('age_veh','count_overpred')

harsh_overpred_comb <- merge(harsh_count,overpred_count)
harsh_overpred_comb$pct_harsh <- harsh_overpred_comb$count_harsh/ harsh_overpred_comb$count_overpred

##For visualization of error comparison ======================================================

# function for number of observations
give.n <- function(x){
  return(c( y = median(x)*1.3, label = length(x)))
  # experiment with the multiplier to find the perfect position
}

# function for median labels
median.n <- function(x){
  return(c(y = median(x)*1.5, label = round(median(x),4)))
  # experiment with the multiplier to find the perfect position
}

plot_data <- in_out_combine

plot_data$diff_ha <- plot_data$ha_ws_avg_ref - plot_data$sale_price
plot_data$diff_cur <- plot_data$cur_ws_avg_ref - plot_data$sale_price

# plot_data$age_veh <- as.factor(plot_data$age_veh)
plot_data <- melt(plot_data,id.vars = 'cur_pred_dir', measure.vars = c('dev_ha','dev_cur'))

# https://stackoverflow.com/questions/5677885/ignore-outliers-in-ggplot2-boxplot
# scale_y_continuous(limits = quantile(dfr$y, c(0.1, 0.9))) - This function removes outliers before calcualting the statistics 
ggplot(plot_data,aes(x=cur_pred_dir, y=abs(value), color=variable)) + geom_boxplot() + coord_cartesian(ylim = quantile((plot_data$value), c(.1, .95))) + labs(title = "MAE using nearest condition approach (100K after removing outliers)", y = 'mae_pct') + stat_summary(fun.data=give.n,geom="text",hjust = -.03,vjust=-4) + stat_summary(fun.data=median.n,geom="text", hjust = 1,vjust=1.5) #

#single
plot_data <- in_out_combine
plot_data$age_veh <- as.factor(plot_data$age_veh)
ggplot(in_out_combine, aes(impact_direction, abs(dev_ha), group=impact_direction)) + geom_boxplot() + stat_summary(fun.data=median.n,geom="text",fun.y='median',colour="blue") + stat_summary(fun.data=give.n,geom="text",fun.y=median) + ggtitle("MAE of h.a. using nearest approach for 100K Sample (Remove OL) ") 
# ggplot(in_out_combine, aes(age_veh, abs(dev_ha), group=impact_direction)) + geom_boxplot() + stat_summary(fun.data=median.n,geom="text",fun.y='median',colour="blue") + stat_summary(fun.data=give.n,geom="text",fun.y=median) + ggtitle("MAE of h.a. using nearest approach for 100K Sample (Remove OL) ") 

## Frequency distribution overall
# geom_histogram(stat='count', fill='blue') for count
plot_data <- in_out_combine

# ggplot(plot_data, aes(x=cur_pred_dir)) + geom_bar(aes(y=(..count..)/sum(..count..)),fill='blue') + labs(title = "Distribution of current prediction direction (100K-ROL)", y = '%of_tot_obs')
# ggplot(plot_data, aes(x=factor(1),fill=cur_pred_dir)) + geom_bar(width = 1) +  coord_polar('y')

#overall
ggplot(plot_data%>% count(cur_pred_dir) %>% mutate(pct=n/sum(n),ypos=cumsum(n) - 0.5*n),aes(cur_pred_dir,n,fill=cur_pred_dir)) + geom_bar(stat='identity') + geom_text(aes(label=paste0(sprintf('%1.1f',pct*100),'%'))) + labs(title = "Distribution of current prediction direction (100K-OL)", y = 'num_obs.')
#cur_direction plu ha_performace Show cout
ggplot(plot_data%>% count(cur_pred_dir,ha_performance) %>% mutate(pct=n/sum(n),ypos=n),aes(x=cur_pred_dir,y=n,fill=ha_performance)) + geom_bar(stat='identity') + geom_text(aes(label=n),position=position_stack(vjust=0.5)) + labs(title = "Ha performance within current prediction direction bin (100K-OL)", y = 'num_obs.')
#show pct
ggplot(plot_data%>% count(cur_pred_dir,ha_performance) %>% mutate(pct=n/sum(n),ypos=n),aes(x=cur_pred_dir,y=n,fill=ha_performance)) + geom_bar(stat='identity') + geom_text(aes(label=n),position=position_stack(vjust=0.5)) + labs(title = "Ha performance within current prediction direction bin (100K-OL)", y = 'num_obs.')

#concentrate on probelm area (Positive impact)
in_out_combine_pos <- subset(in_out_combine,impact_direction == 1)
ggplot(in_out_combine_pos, aes(age_veh, (dev_ha), group=age_veh)) + geom_boxplot() + stat_summary(fun.data=median.n,geom="text",fun.y='median',colour="blue") + stat_summary(fun.data=give.n,geom="text",fun.y=median) + ggtitle("Error of h.a. using nearest approach for 100K Sample (Positive impact only) ") 
ggplot(in_out_combine_pos, aes(cur_pred_dir, abs(dev_ha), group=cur_pred_dir)) + geom_boxplot() + stat_summary(fun.data=median.n,geom="text",fun.y='median',colour="blue") + stat_summary(fun.data=give.n,geom="text",fun.y=median) + ggtitle("Error of h.a. using nearest approach for 100K Sample (Positive impact only) ") 

plot_pos <- in_out_combine_pos
plot_pos$age_veh <- as.factor(plot_pos$age_veh)
plot_pos <- melt(plot_pos, id.vars = 'cur_pred_dir', measure.vars = c('dev_ha','dev_cur'))
ggplot(plot_pos,aes(x=cur_pred_dir, y=abs(value), color=variable)) + geom_boxplot()  + labs(title = "ME using nearest condition approach (100K after removing outliers)") #+ stat_summary(fun.data=median.n,geom="text",fun.y='median',colour="blue") #+ stat_summary(fun.data=give.n,geom="text",fun.y=median)  #

## Frequency distribution

ggplot(plot_pos,aes(cur_pred_dir, group = age_bin)) + geom_bar(aes(y=(..count..)/sum(..count..)),fill='blue') + labs(title = "Frequency of direction of current prediction (100K - Positive impact only)", y = '%obs')
#try group
ggplot(in_out_combine_pos,aes(x=age_bin, group =(cur_pred_dir))) + geom_histogram(position = position_dodge(width = 0.5), stat='count') + labs(title = "Frequency of direction of current prediction (100K - Positive impact only)", y = 'count')
#ggplot(in_out_combine,aes(cur_pred_dir)) + geom_bar(aes(y=(..count..)/sum(..count..)),fill='blue')  + labs(title = "Frequency of direction of current prediction (100K - remove OL)", y = '%obs')
ggplot(in_out_combine_pos,aes(x=age_veh, fill =cur_pred_dir)) + geom_bar() + labs(title = "Frequency of direction of current prediction (100K - pos)", y = '%obs')






#aggregate visulalization 

ggplot(error_incident_100K_v13, aes(x=description, y=ha_rmse_pct_sale))+ geom_bar(stat='identity') + theme(axis.text.x = element_text(angle = 90, hjust=1)) + labs(title = "Ha_RMSE percentage of sale price", x = 'incident', y = 'rmse % of sale') 
ggplot(error_incident_100K_v13, aes(x=description, y=ha_rmse_pct_sale))+ geom_point(aes(size=pct_av_abs_hadj)) + theme(axis.text.x = element_text(angle = 90, hjust=1)) + labs(title = "Ha_RMSE percentage of sale price (rm: dev_cur >= 1.5)", x = 'incident', y = 'rmse % of sale') 

#rmse_pct_of_sale_obs
ggplot(error_incident_100K_v10, aes(x=description, y=ha_rmse_pct_sale))+ geom_point(aes(size=num_obs)) + theme(axis.text.x = element_text(angle = 90, hjust=1)) + labs(title = "Ha_RMSE percentage of sale price", x = 'incident', y = 'rmse % of sale') 

#rmse_pct_of_sale_obs
ggplot(error_incident_100K_v10, aes(x=description, y=ha_rmse_pct_sale))+ geom_point(aes(size=num_obs)) + theme(axis.text.x = element_text(angle = 90, hjust=1)) + labs(title = "Ha_RMSE percentage of sale price (rm: dev_cur >= 1.5)", x = 'incident', y = 'rmse % of sale')

#ME size_obs
ggplot(error_incident_100K_v10, aes(x=description, y=me_pct_cur))+ geom_point(aes(size=num_obs)) + theme(axis.text.x = element_text(angle = 90, hjust=1)) + labs(title = "Me  (rm: dev_cur >= 1.5)", x = 'incident', y = 'rmse % of sale')


#overall 
#in_out_combine_remOutlier <- subset(in_out_combine, abs(dev_cur) < .8 )
ggplot(in_out_combine, aes(x=age_veh, y=abs(dev_ha),group=age_veh))+ geom_boxplot() +  labs(title = "After removing outlier", x = 'age of vehicle', y = 'ha_abs_error_pct') 
ggplot(in_out_combine,aes(age_veh,dev_ha,group=age_veh))+geom_boxplot()+labs(title="Historic Adjusted Error")

## small sample test harness ==========================================================================================



#get the required functions 
source('source_v13.R')

impact_event  <- fn_event_impact_calc(input_vehicle,input_history, group_table ,category_table, impact_table, range_table)

# This block is for data  load reduction 
impact_event_col_red <- select(impact_event_100K,c(vin,sale_date,ws_avg_pr,category_id, subcategory_id, range_adj_dollar,override))

# comb_event_price <- merge(impact_event_col_red,input_vehicle, by.x = c('vin','sale_date','ws_avg_pr'), by.y = c('vin','sale_date','ws_avg_pr'))
# comb_event_price <- select(comb_event_price,-c(ownerCount,model_yr))
# ifelse(nrow(comb_event_price)==nrow(impact_event),"Data matches!!","MISMATCH!!CAREFUL!!!")

# End block data load reduction





### Impact analysis ===============================================================

# # First need to remove vehicles with total code
# vin_no_override <- subset(vin_override_count_100K, count_override == 0)
# #in few cases it may select vin with override code in other sale date
# in_out_combine_nt <- subset(in_out_combine, vin %in% vin_no_override$vin) 
# 
# # merge with all events 
# impact_event_not_tot <- merge(in_out_combine_nt,impact_event_100K)
# 
# left_over_overrides <- subset(impact_event_not_tot, override ==1)
# impact_event_not_tot <- subset(impact_event_not_tot, !(vin %in% left_over_overrides$vin)) # may clean little more
# 
# rm(left_over_overrides) # memory cleaning 
# 
# # find the seg_adj_factor to adjust cateogry level impact
# impact_event_not_tot$seg_adj_factor <- impact_event_not_tot$seg_adj_impact_ws_avg /impact_event_not_tot$impact_before_seg_adj
# impact_event_not_tot$seg_adj_factor <- ifelse(is.nan(impact_event_not_tot$seg_adj_factor),1,impact_event_not_tot$seg_adj_factor)
# impact_event_not_tot$seg_adj_subcat_impact <- impact_event_not_tot$range_adj_dollar * impact_event_not_tot$seg_adj_factor 


source('D:/Nasrul/my_projects/vehicle_history/test_10K_plus_error/source_v13.R')
cl <- makeCluster(numCore-5)
registerDoParallel(cl)

#make the necessary objects available to all cores 
clusterExport(cl,c('impact_event_not_tot','impact_event_100K', 'fn_category_impact','fn_count_totaled_code'),envir = environment())

#pass all required functions and libraries to all cores 
invisible(clusterEvalQ(cl, 'source_v13.r'))
invisible(clusterEvalQ(cl, library(plyr)))
invisible(clusterEvalQ(cl, library(dplyr)))
#invisible(clusterEvalQ(cl, library(lubridate)))

start <- Sys.time()
#we may not get the same number of row as in input_price table as data may have duplicate entries (AUTOCHECK_VEHICLE has duplicates vins)
# make sure right df is passed: with or w/o totaled 
category_impact_100K = ddply(impact_event_100K, .(vin, sale_date, ws_avg_pr), function(x) fn_category_impact(x) , .parallel = T)

time_taken <- Sys.time() - start
time_taken

write.csv(category_impact_100K, 'category_impact_100K.csv', row.names = F)

# wy can't I parallelize???
# vin_override_count_100K <- ddply(impact_event_100K, .(vin,sale_date,ws_avg_pr), function(x) fn_count_totaled_code (x), .parallel = T)

stopCluster(cl)

category_name <- data.frame('category_id' = c(0,1,10,19,20,21,22,23,24,25,26,27,28,29,30,40,41,42,43,44,50,99),
                            'category_name' = c('ownerCount','accidentRelated','FTHW','recall','lemon','grey_mkt','repossed','abandoned','odomenter_issue','title_issue','import','missing_parts','rebuilt','repair','Misc.','CPO','warranty','emission','service','inspection','use','totaled'))

category_impact_100K <- merge(category_impact_100K,category_name)

# vin_override_count_100K <- ddply(impact_event_100K, .(vin,sale_date,ws_avg_pr), summarise, count_override = sum(override))
# write.csv(vin_override_count_100K,'vin_override_count_100K.csv', row.names = F)
#vin_override_count_100K <- merge(vin_override_count_100K,category_name)

sample_totaled_100K <- subset(vin_override_count_100K,count_override >0)
sample_not_totaled_100K <- subset(vin_override_count_100K,count_override ==0)

in_out_combine_nt <- merge(sample_not_totaled_100K,in_out_combine) # already removed defined outliers 
in_out_combine_nt <- merge(in_out_combine_nt,category_impact_100K)
in_out_combine_nt <- unique(in_out_combine_nt)

in_out_combine_nt$seg_adj_factor <- in_out_combine_nt$seg_adj_impact_ws_avg /in_out_combine_nt$impact_before_seg_adj
in_out_combine_nt$seg_adj_factor <- ifelse(is.nan(in_out_combine_nt$seg_adj_factor),1,in_out_combine_nt$seg_adj_factor)
in_out_combine_nt$seg_adj_cat_impact <- in_out_combine_nt$cat_impact * in_out_combine_nt$seg_adj_factor
in_out_combine_nt$pct_seg_adj_impact <- in_out_combine_nt$seg_adj_cat_impact/in_out_combine_nt$ws_avg_pr

# in_out_combine_nt$positive_impact <- ifelse(in_out_combine_nt$seg_adj_cat_impact >=0,1,0) # changed from overall impact based flag


fn_pct_impact_analysis <- function(in_out_combine_nt, group1, group2){
  result <- ddply(in_out_combine_nt,c(group1,group2), summarise, av_pct_impact = mean(pct_seg_adj_impact), num_obs = sum(ha_better_or_eq>-1))
  
  result <- reshape(result, idvar = group1, timevar = group2, direction = "wide")
  
}

fn_pct_impact_overall_analysis <- function(in_out_combine_nt, group1){
  result <- ddply(in_out_combine_nt,c(group1), summarise, av_dol_impact = mean(seg_adj_impact_ws_avg_r25), num_obs = sum(ha_better_or_eq>-1), av_ws_base = mean(ws_avg_pr), pct_impact = av_dol_impact/av_ws_base, av_sale_pr = mean(sale_price),ws_avg_ha_pr = mean(ws_avg_ha_pr), pct_better_or_eq = mean(ha_better_or_eq))
  
  #result <- reshape(result, idvar = group1, timevar = group2, direction = "wide")
  
}


impact_age_100K_nt <- fn_pct_impact_analysis(in_out_combine_nt,'age_veh','category_name')

impact_seg_code_100K_nt <- fn_pct_impact_analysis(in_out_combine_nt,'seg_code','category_name')

impact_seg_group_100K_nt <- fn_pct_impact_analysis(in_out_combine_nt,'seg_group','category_name')

impact_sale_price_bin_100K_nt <- fn_pct_impact_analysis(in_out_combine_nt,'sale_price_bin','category_name')

impact_impact_direction_100K_nt <- fn_pct_impact_analysis(in_out_combine_nt,'impact_direction','category_name')


impact_age_overall_100K <- fn_pct_impact_overall_analysis(in_out_combine,'age_veh')

impact_seg_code_overall_100K <- fn_pct_impact_overall_analysis(in_out_combine,'seg_code')

impact_seg_group_overall_100K <- fn_pct_impact_overall_analysis(in_out_combine,'seg_group')

impact_sale_price_bin_overall_100K <- fn_pct_impact_overall_analysis(in_out_combine,'sale_price_bin')



wb_impact <- createWorkbook()

addWorksheet(wb_impact,'overall_impact_age')
addWorksheet(wb_impact,'overall_impact_seg_code')
addWorksheet(wb_impact,'overall_impact_seg_group')
addWorksheet(wb_impact,'overall_impact_price_bin')

addWorksheet(wb_impact, 'impact_age')
addWorksheet(wb_impact,'impact_seg_code')
addWorksheet(wb_impact,'impact_seg_group')
addWorksheet(wb_impact,'impact_sale_price_bin')
addWorksheet(wb_impact,'impact_direction')

writeData(wb_impact,'overall_impact_age',impact_age_overall_100K, rowNames = F)
writeData(wb_impact,'overall_impact_seg_code',impact_seg_code_overall_100K, rowNames = F)
writeData(wb_impact,'overall_impact_seg_group',impact_seg_group_overall_100K, rowNames = F)
writeData(wb_impact,'overall_impact_price_bin',impact_sale_price_bin_overall_100K, rowNames = F)



writeData(wb_impact,'impact_age',impact_age_100K_nt, rowNames = F)
writeData(wb_impact,'impact_seg_code',impact_seg_code_100K_nt, rowNames = F)
writeData(wb_impact,'impact_seg_group',impact_seg_group_100K_nt, rowNames = F)
writeData(wb_impact,'impact_sale_price_bin',impact_sale_price_bin_100K_nt, rowNames = F)
writeData(wb_impact,'impact_direction',impact_impact_direction_100K_nt, rowNames = F)

saveWorkbook(wb_impact,'impact_analysis_100K_remTotaled_v13_08.11.xlsx')

#saveWorkbook(wb_impact,'impact_analysis_100K_v10_8.01.xlsx')

#category-wise impact
ggplot(in_out_combine_nt,aes(category_name,pct_seg_adj_impact, group=category_name))+geom_boxplot() + theme(axis.text.x = element_text(angle = 90, hjust=1)) + labs(title='Percentage impact of categories (no totaled)') +  stat_summary(fun.data=give.n,geom="text",hjust = 0,vjust=-3) + stat_summary(fun.data=median.n,geom="text", hjust = 1,vjust=1)

#age_group impact
xx <- ddply(in_out_combine_nt, .(age_veh,category_name), summarise, av_seg_adj_impact = mean(pct_seg_adj_impact), num_obs = sum(!is.na(pct_seg_adj_impact)))
ggplot(xx,aes(age_veh,av_seg_adj_impact, fill=category_name))+geom_bar(stat='identity')  + labs(title='Av. Percentage impact of categories in age group (no totaled)') + geom_text(aes(label=sprintf('%0.2f',av_seg_adj_impact)),position=position_stack(vjust=.5))        #+ theme(axis.text.x = element_text(angle = 0, hjust=1))

#seg_code impact
xx <- ddply(in_out_combine_nt, .(seg_code,category_name), summarise, av_seg_adj_impact = mean(pct_seg_adj_impact), num_obs = sum(!is.na(pct_seg_adj_impact)))
ggplot(xx,aes(seg_code,av_seg_adj_impact, fill=category_name))+geom_bar(stat='identity')  + labs(title='Av. Percentage impact of categories in seg_code (no totaled)') + geom_text(aes(label=sprintf('%0.2f',av_seg_adj_impact)),position=position_stack(vjust=.5))        #+ theme(axis.text.x = element_text(angle = 0, hjust=1))

#seg_group impact
xx <- ddply(in_out_combine_nt, .(seg_group,category_name), summarise, av_seg_adj_impact = mean(pct_seg_adj_impact), num_obs = sum(!is.na(pct_seg_adj_impact)))
ggplot(xx,aes(seg_group,av_seg_adj_impact, fill=category_name))+geom_bar(stat='identity')  + labs(title='Av. Percentage impact of categories in seg_code (no totaled)') + geom_text(aes(label=sprintf('%0.2f',av_seg_adj_impact)),position=position_stack(vjust=.5))        #+ theme(axis.text.x = element_text(angle = 0, hjust=1))
