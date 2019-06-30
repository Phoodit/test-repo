# -- package & init 

options(java.parameters = "-Xmx2048m")
library(RODPS)
library(data.table)
library(readr)
library(purrr)
library(haven)
library(readxl)
library(dplyr)
library(tidyr)
library(magrittr)
library(ggplot2)
library(stringr)
rodps.init("C:/Program Files/R/odps_config.ini")
cp <- function(x,row.names = FALSE,col.names = TRUE,...) {
  write.table(x,file = paste0("clipboard-", 2^14),sep = "\t",row.names = row.names,col.names = col.names,...)
  print("Successfully copied to clipboard")}
windowsFonts(YaHei = windowsFont("微软雅黑"))

# --- init data import 
  path <- 'D://1 零售通 LST/1.1 计划 Planning/1.1.3 R Python 和其他分析工具/1. R_Projects/Work/BCW_forecast/2018_618_forecast/Data'
  path %>% setwd()
  # from ODPS to local 
  
  bcw1 <- rodps.read.table('aaronh_bcw_618_1')
  bcw2 <- rodps.read.table('aaronh_bcw_618_2')
  bcw3 <- rodps.read.table('aaronh_bcw_618_3')
  bcw4 <- rodps.read.table('aaronh_bcw_618_4')
  bcw5 <- rodps.read.table('aaronh_bcw_618_5')
  bcw6 <- rodps.read.table('aaronh_bcw_618_6')
  write_csv(bcw1,'bcw1')
  write_csv(bcw2,'bcw2')
  write_csv(bcw3,'bcw3')
  write_csv(bcw4,'bcw4')
  write_csv(bcw5,'bcw5')
  write_csv(bcw6,'bcw6')


#--- 1. compare 618 2018 vs 2019
  # first do the SKU mapping 
  
  
  
  
  bcw1 %>% names
  bcw1 %>% filter(gmv_base_18 > 0) %>% select(cspu_id,cspu_name) %>% unique -> SKU_18 
  bcw1 %>% filter(gmv_base_19 > 0) %>% select(cspu_id,cspu_name) %>% unique -> SKU_19 
  SKU_18 %>% count
  SKU_19 %>% count
  intersect(SKU_18,SKU_19)
  union(SKU_18,SKU_19) %>% count()
  SKU_18 %>% cp
  SKU_19 %>% cp
  bcw4 <- rodps.read.table('aaronh_bcw_618_4')
  bcw4 %>% select(cspu_id,cspu_name) %>% unique %>%  cp
  bcw1 %>% filter(gmv_base_19 > 0) %>% cp 
  
  # import the mapping list 
  setwd('C://Users/phoodit.photrakanp/Desktop')
  SKU_map <- read_excel('百草味2018-19单品对应.xlsx')
  
  # ABC rank 
  bcw1 %>% filter(gmv_base_18 > 0) %>% group_by(cspu_id,cspu_name) %>% 
    summarize(gmv = sum(gmv_base_18)) %>% arrange(desc(gmv)) -> SKU_18
  bcw1 %>% filter(gmv_base_19 > 0) %>% group_by(cspu_id,cspu_name) %>% 
    summarize(gmv = sum(gmv_base_19)) %>% arrange(desc(gmv)) -> SKU_19
  SKU_18$cumsum <- cumsum(SKU_18$gmv/sum(SKU_18$gmv))
  SKU_19$cumsum <- cumsum(SKU_19$gmv/sum(SKU_19$gmv))
  SKU_18$rank_18 <- rank(-SKU_18$gmv)/length(SKU_18$gmv)
  SKU_19$rank_19 <- rank(-SKU_19$gmv)/length(SKU_19$gmv)
  SKU_18 <- SKU_18 %>% mutate(grade = case_when( rank_18 < 0.21 ~ 'A'
                                           ,rank_18 >= 0.21 & rank_18 < 0.51 ~ 'B'
                                           ,TRUE ~ 'C')) 
  SKU_19 <- SKU_19 %>% mutate(grade = case_when( rank_19 < 0.21 ~ 'A'
                                                  ,rank_19 >= 0.21 & rank_19 < 0.51 ~ 'B'
                                                  ,TRUE ~ 'C'))
  SKU_18rank <- SKU_18 ; SKU_19rank <- SKU_19
  
  #-- lift up by grade
  bcw1 %>% filter(gmv_base_18 > 0) %>% group_by(cspu_id,cspu_name) %>% 
    summarize(gmv_avg_18 = sum(gmv_base_18)/59,gmv_618_18 = sum(gmv_618_18)) %>% arrange(desc(gmv_avg_18)) -> SKU_18
  SKU_18rankC <- SKU_18rank %>% select(cspu_id,grade)
  gradeLift  <- SKU_18 %>% left_join(SKU_18rankC,by = 'cspu_id')
  gradeLift %>% names
  
  gradeLift <- gradeLift %>% group_by(grade) %>% summarise(liftRate_18_grade = sum(gmv_618_18)/sum(gmv_avg_18))
    
  #-- put it all together 
  
  #-- lift up rate 
  
  bcw1 %>% filter(gmv_base_18 > 0) %>% group_by(cspu_id,cspu_name) %>%
    summarize(gmv_avg_18 = sum(gmv_base_18)/59,gmv_618 = sum(gmv_618_18)) %>% arrange(desc(gmv_avg_18)) -> SKU_18
  SKU_18 <- SKU_18 %>% mutate(liftRate_18 = gmv_618/gmv_avg_18) 
  SKU_18rankC <- SKU_18rank %>% select(cspu_id,grade)
  SKU_18 <- SKU_18 %>% left_join(SKU_18rankC, by = 'cspu_id') 
  names(SKU_18)[c(1,2,6)] <- c('cspu_id_18','cspu_name_18','grade_18')
 
  bcw1 %>% filter(gmv_base_19 >0) %>% group_by(cspu_id,cspu_name) %>% 
    summarize(gmv_avg_19 = sum(gmv_base_19)/59) %>% arrange(desc(gmv_avg_19)) -> SKU_19
  SKU_19rankC <- SKU_19rank %>% select(cspu_id,grade) 
  SKU_19 <- SKU_19 %>% left_join(SKU_19rankC, by = 'cspu_id')
  names(SKU_19) <- c('cspu_id_19','cspu_name_19','gmv_avg_19','grade_19') 
  
  mapTab <- SKU_19 %>% left_join(SKU_map, by = c('cspu_id_19','cspu_name_19'))  
  mapTab <- mapTab %>% left_join(SKU_18,by = c('cspu_id_18','cspu_name_18'))
  mapTab <- mapTab %>% left_join(gradeLift,by = c('grade_19' = 'grade'))
  
  #-- fix NA data, new item by using grade lift and project  
  mapTab <- mapTab %>% mutate( fix_liftRate = ifelse(is.na(cspu_id_18),liftRate_18_grade,liftRate_18))
  mapTab <- mapTab %>% mutate( proj618_1 = gmv_avg_19*fix_liftRate )
  
  6000000/sum(mapTab$proj618_1) 
  8000000/sum(mapTab$proj618_1) 
  mapTab$proj618_600 <- mapTab$proj618_1/sum(mapTab$proj618_1) * 6000000    
  mapTab$proj618_800 <- mapTab$proj618_1/sum(mapTab$proj618_1) * 8000000

# 2. double 11 lift rate for reference 
  
  bcw2 <- rodps.read.table('aaronh_bcw_618_2')
  bcw2 %>% dim  
  names(bcw2)[c(5,6)] <- c('gmv_base_1111','gmv_1111_18')
  
  # ranking, grading SKU in 1111 
  bcw2 %>% filter(gmv_base_1111 > 0 ) %>% group_by(cspu_id,cspu_name) %>% 
    summarise(gmv_base_1111 = sum(gmv_base_1111)/35,gmv_1111_18 = sum(gmv_1111_18)) %>% arrange(desc(gmv_base_1111)) -> SKU_1111
  SKU_1111 <- SKU_1111 %>% mutate(liftRate11 = gmv_1111_18/gmv_base_1111) 
  SKU_1111$cumsum <- cumsum(SKU_1111$gmv_base_1111)/sum(SKU_1111$gmv_base_1111)
  SKU_1111$rank <- rank(-SKU_1111$gmv_base_1111)/length(SKU_1111$gmv_base_1111)
  SKU_1111 <- SKU_1111 %>% mutate(grade = case_when( rank < 0.21 ~ 'A'
                                                     ,rank >= 0.21 & rank < 0.51 ~ 'B'
                                                     ,TRUE ~ 'C')
                                  ) 
  # AB SKU in 1111 contribute much higher 
  SKU_1111 %>% group_by(grade) %>% summarise(liftRate11_grade = sum(gmv_1111_18)/sum(gmv_base_1111))
  SKU_1111 %>% group_by(grade) %>% summarise(sum(gmv_base_1111))
  
  
  # checking the cross-intersect SKU for 18 618 vs 18 1111 and 19 618 and 18 1111
  SKU_1111 %>% filter(grade != 'C') -> SKUab_1111 
  SKU_18 %>% filter(grade_18 != 'C') -> SKUab_18
  intersect(SKUab_18$cspu_id_18,SKUab_1111$cspu_id) -> itsSKU
  itsSKU %>% length()
  SKU_18 %>% filter(cspu_id_18 %in% itsSKU) -> itsSKU_18
  SKU_1111 %>% select(cspu_id,gmv_base_1111) -> SKU_1111_2
  itsSKU_18 %>% left_join(SKU_1111_2,by = c('cspu_id_18' = 'cspu_id')) -> itsSKU_18
  sum(itsSKU_18$gmv_base_1111) / sum(itsSKU_18$gmv_avg_18)
  sum(SKU_1111$gmv_base_1111) / sum(SKU_18$gmv_avg_18)
  sum(SKU_1111$gmv_base_1111) / sum(SKU_19$gmv_avg_19)
  
  
  # 1111 has much fewer shared SKU with 19, seasonality in base is very clear. 
  SKU_19 %>% filter(grade_19 != 'C') -> SKUab_19
  intersect(SKUab_19$cspu_id_19,SKUab_1111$cspu_id) -> itsSKU2
  itsSKU2 %>% length
  SKU_19 %>% filter(cspu_id_19 %in% itsSKU2) -> itsSKU_19
  itsSKU_19 %>% left_join(SKU_1111_2,by = c('cspu_id_19' = 'cspu_id')) -> itsSKU_19
  
  # mapping category lv2 
  bcw6 <- rodps.read.table('aaronh_bcw_618_6')
  cateMap <- bcw6[,c(1,3)] # move out cspu_name
  cateMap <- cateMap[!duplicated(cateMap),] # remove duplicated, but still there are duplicate of cspu_id by cate name change 
  cateMap %>% group_by(cspu_id) %>% count() %>% setDT %>% .[n == 2] -> dubCspu 
  cateMap %>% setDT
  cateMap[cspu_id %in% dubCspu$cspu_id,"cate_lv2_name"] <- '方便速食' # 方便速食和方便食品
  cateMap <- cateMap[!duplicated(cateMap),] 
  
  
  SKU_18 <- SKU_18 %>% left_join(cateMap,by = c('cspu_id_18' = 'cspu_id'))
  SKU_19 <- SKU_19 %>% left_join(cateMap,by = c('cspu_id_19' = 'cspu_id'))
  SKU_1111 <- SKU_1111 %>% left_join(cateMap,by = c('cspu_id' = 'cspu_id'))
  mapTab <- mapTab %>% left_join(cateMap,by = c('cspu_id_19' = 'cspu_id'))
  
  (mapTab$cspu_id_19 %>% unique %>% length) == (mapTab$cspu_id_19 %>% length) # validation
  cateSumm <- mapTab %>% group_by(cate_lv2_name) %>%
    summarize(gmv_avg_19 = sum(gmv_avg_19),gmv_avg_18 = sum(gmv_avg_18,na.rm = TRUE),gmv_618 = sum(gmv_618,na.rm = TRUE),liftRate_18 = sum(gmv_618,na.rm = TRUE)/sum(gmv_avg_18,na.rm = TRUE))
  cateSumm <- cateSumm %>% mutate(proj_cate_618 = liftRate_18 * gmv_avg_19)
  cateSumm$proj_cate_618 %>% sum(na.rm = TRUE)
  
  cateSumm_11 <- SKU_1111 %>% group_by(cate_lv2_name) %>% 
    summarize(gmv_base_1111 = sum(gmv_base_1111),gmv_1111_18 = sum(gmv_1111_18),liftRate11 = sum(gmv_1111_18)/sum(gmv_base_1111) )
  cateSumm_11 <- cateSumm_11 %>% mutate( proj_cate_11 = gmv_base_1111*liftRate11)
  
  cateSumm_both <- cateSumm %>% left_join(cateSumm_11,by = 'cate_lv2_name')
  cateSumm_both <- cateSumm_both %>% mutate(compRate = proj_cate_618/proj_cate_11)
  
  # 3. divide to each warehouse by proportion 
  
  
  whProp <- bcw1 %>% select(-c(gmv_base_18,gmv_618_18)) %>%  filter(gmv_base_19 > 0) %>%  group_by(cspu_id) %>% mutate(whProp = gmv_base_19/sum(gmv_base_19))
  mapTabOut <- mapTab %>% setDT %>% .[,c(1,14)]
  mapTabOut$cspu_id_19 %>% duplicated
  whProp <- whProp %>% left_join(mapTabOut,c('cspu_id' = 'cspu_id_19'))
  outTab <- whProp %>% mutate(forecast = proj618_600 * whProp)
  outTab %>% cp
  
  
  bcw3 <- rodps.read.table('aaronh_bcw_618_3')
  bcw3 %>% dim
  bcw3 %>% names  
  
  
  
  
    
  