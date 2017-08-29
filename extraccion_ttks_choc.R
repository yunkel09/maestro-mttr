#   ____________________________________________________________________________
#   ARCHIVO MAESTRO CHOC A BD                                               ####

  # paquetes necesarios
  paquetes <- c('lubridate',
                'tidyverse',
                'magrittr',
                'data.table',
                'stringr',
                'RODBC')
        
  # cargar librerías
  sapply(paquetes, require, character.only = TRUE, quiet = TRUE)
  
  # cargar funciones
  source('custom_functions.R')
  
  # borrar listado de paquetes
  rm(paquetes)
  
  
##  ............................................................................
##  DEFINIR ORDEN DE COLUMNAS ARCHIVO FINAL                                 ####

  orden_cols <- c('tkstatusid', 'ticketid', 'description', 'reportdate',
                  'changedate', 'internalpriority', 'siteid', 'ownergroup',
                  'decimal_duration', 'new_decimal_duration', 'mttr_valido_ts',
                  'empresa', 'sede', 'area', 'target', 'region', 'rca_ts',
                  'ttk_tigo_star', 'reportedpriority')

##  ............................................................................
##  DESCARGAR ARCHIVOS                                                      ####

  # data general
  general <- fread(input            = './files/historico_ttks/01.- historico_mttr.csv', 
                   stringsAsFactors = FALSE,
                   data.table       = FALSE)


  # cargar tabla lookup de ownergroups que deben compararse
  info.grupos <- fread(input      = './files/06.- info_grupos.csv',
                       data.table = FALSE)
  
  
##  ............................................................................
##  TRANSFORMAR ARCHIVO BUC                                                 ####

  general.1 <- general %>%
               mutate_at(vars(reportdate, changedate),
                         ymd_hm, tz = 'America/Guatemala')
  
  # seleccionar los ttks que sean de 2017 y que no pertenezcan a TH
  general.2 <- general.1 %>%
               filter(reportdate > '2016-12-31 23:59', siteid != 'TH')
  
  
  # seleccionar todos los ttks de 2016, incluyendo los de TH
  general.3 <- general.1 %>%
               filter(reportdate < '2016-12-31 23:59')
              
  # unificar pero sin ttks de TH en 2017
  general.4 <- bind_rows(general.3, general.2) %>%
               arrange(reportdate) %>%
               mutate(mttr_valido_ts = NA)
  
  # leer data de th pre-procesada
  th        <- fread(input            = './files/general_th.csv', 
                     stringsAsFactors = FALSE,
                     data.table       = FALSE)
  
  col_order <- c('ticketid',
                 'description',
                 'reportdate',
                 'internalpriority',
                 'changedate',
                 'ownergroup',
                 'siteid',
                 'decimal_duration',
                 'mttr_valido_ts')
  
  
  
  # seleccionar solo los que estan entre 01-Enero-17 al 15-Julio-17 para que
  # coincidan con los datos para hacer pruebas.
  
  th.1 <- th %>%
          mutate(internalpriority = NA) %>%
          rename(mttr_valido_ts = mttr_valido) %>%
          mutate_at(vars(reportdate, changedate),
                         dmy_hm, tz = 'America/Guatemala') %>%
          filter(reportdate < '2017-07-15 17:04:00') %>%
          select(col_order)
  
  
  # La parte que va del 15-Julio-17 hasta el 25-Agosto-2017 se agregara
  # posteriormente en las pruebas.
  
  th.2 <- th %>% mutate(internalpriority = NA) %>%
                 rename(mttr_valido_ts = mttr_valido) %>%
                 mutate_at(vars(reportdate, changedate),
                                dmy_hm, tz = 'America/Guatemala') %>%
                 filter(reportdate > '2017-07-15 17:04:00') %>%
                 select(col_order)
  
                 
  # Bindear el archivo general con los datos de tigo star ya incluyendo las
  # columnas nuevas que trae th
  
  tmp <- bind_rows(general.4, th.1)               
  
  
  # fusionar todo y agregar la info de los grupos.        
  maestro       <-      tmp %>%
                        arrange(reportdate) %>%
                        mutate(tkstatusid = row.names(.),
                               new_decimal_duration = decimal_duration) %>%
                        left_join(info.grupos, by = 'ownergroup') %>%
                        mutate(rca_ts = NA, ttk_tigo_star = NA) %>%
                        mutate_at('tkstatusid', as.integer) %>%
                        select(orden_cols) %>%
                        mutate_if(is.character, toupper) %>%
                        mutate_at('description', str_sub, star = 1, end = 40)
                        
              
  # cambiar maestro para que sea posible guardarlo en la base de datos
  maestro.1 <- maestro %>%
               mutate_at(vars(reportdate, changedate), as.character)

  # borrar el resto de df
  rm(list = setdiff(ls(), c('incidents', 'ttk')))
  
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



        
        
        