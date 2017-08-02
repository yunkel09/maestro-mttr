#   ____________________________________________________________________________
#   ARCHIVO MAESTRO CHOC A BD                                               ####

  # paquetes necesarios
  paquetes <- c('lubridate', 'tidyverse', 'magrittr', 'data.table')
        
  # cargar librerías
  sapply(paquetes, require, character.only = TRUE)
  
  # cargar funciones
  source('custom_functions.R')
  
  # borrar listado de paquetes
  rm(paquetes)
  
  
##  ............................................................................
##  DEFINIR ORDEN DE COLUMNAS ARCHIVO FINAL                                 ####

  orden_cols <- c('tkstatusid', 'ticketid', 'description', 'reportdate', 'changedate',
                  'internalpriority', 'siteid', 'ownergroup', 'decimal_duration',
                  'new_decimal_duration', 'empresa', 'sede', 'area', 'target', 'region',
                  'rca_ts', 'ttk_tigo_star')

##  ............................................................................
##  DESCARGAR ARCHIVOS                                                      ####

  # data general
  general <- fread(input            = './files/historico_ttks/01.- historico_mttr.csv', 
                   stringsAsFactors = FALSE,
                   data.table       = FALSE)

  # data con problemas en el formato de fecha
  buc <-     fread(input = './files/historico_ttks/02.- ttks_buc.csv',
                   data.table = FALSE,
                   stringsAsFactors = FALSE) %>% as.tibble

  # cargar tabla lookup de ownergroups que deben compararse
  info.grupos <- fread(input      = './files/06.- info_grupos.csv',
                       data.table = FALSE)
  
  
##  ............................................................................
##  TRANSFORMAR ARCHIVO BUC                                                 ####

  
  # modificar registros de forma manual
  buc[buc$changedate == 'Apr 1, 2016 7:09 AM', 'changedate'] <- 'Apr 01, 2016 7:09 AM'
  buc[buc$changedate == '11/03/2016', 'changedate']          <- 'Mar 11, 2016 08:00 AM'


  # corregir fechas en archivo buc
  buc.1 <-buc %>% 
          mutate_at(vars(reportdate, changedate), corregir_fecha) %>%
          arrange(reportdate)

  # cambiar formato de fecha en archivo general
  general.1 <-  general %>%
                mutate_at(vars(reportdate, changedate),
                          dmy_hm, tz = 'America/Guatemala')

  # fusionar todo        
  maestro       <-      bind_rows(general.1, buc.1) %>%
                        mutate(tkstatusid = row.names(.),
                               new_decimal_duration = decimal_duration) %>%
                        left_join(info.grupos, by = 'ownergroup') %>%
                        mutate(rca_ts = NA, ttk_tigo_star = NA) %>%
                        arrange(reportdate) %>%
                        mutate_at('tkstatusid', as.integer) %>%
                        select(orden_cols) %>%
                        mutate_if(is.character, toupper) %>%
                        mutate_at('description', str_sub, star = 1, end = 40)
  
  
  # Este df es para guardarse en base de datos
  maestro.1 <- maestro %>% mutate_at(vars(reportdate, changedate), as.character)
                      
                

# borrar el resto de df
rm(list = setdiff(ls(), c('maestro.1', 'maestro')))

# abrir conexion 
con       <-  odbcConnect(dsn = 'yunkel',
                          uid = 'wchavarria',
                          pwd = 'Tigo1234.')



# guardar la primera vez con append = FALSE
sqlSave(channel   = con,
        dat       = maestro.1,
        tablename = 'MTTR',
        rownames  = FALSE,
        append    = FALSE)



# close con
close(con)



        
        
        