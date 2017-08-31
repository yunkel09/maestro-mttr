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
  
  # opciones generales
  options(str = strOptions(vec.len = 2))
  

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

  # convertir fechas a POSXCI
  general %<>% mutate_at(vars(reportdate, changedate),
                         mdy_hm, tz = 'America/Guatemala')
  
  # seleccionar los ttks que sean de 2017 y que no pertenezcan a TH
  general.1 <- general %>%
               filter(reportdate > '2016-12-31 23:59', siteid != 'TH')
  
  
  # seleccionar todos los ttks de 2016, incluyendo los de TH
  general.2 <- general %>%
               filter(reportdate < '2016-12-31 23:59')
              
  # unificar pero sin ttks de TH en 2017
  general.3 <- bind_rows(general.1, general.2) %>%
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
          filter(reportdate <= '2017-07-15 17:04:00') %>%
          select(col_order)
  
  
  # La parte que va del 15-Julio-17 hasta el 25-Agosto-2017 se agregara
  # posteriormente en las pruebas.
  
  th.2 <- th %>% 
          mutate(internalpriority = NA) %>%
          rename(mttr_valido_ts = mttr_valido) %>%
          mutate_at(vars(reportdate, changedate),
                    dmy_hm, tz = 'America/Guatemala') %>%
          filter(reportdate > '2017-07-15 17:04:00') %>%
          select(col_order)
  
                 
  # Bindear el archivo general con los datos de tigo star ya incluyendo las
  # columnas nuevas que trae th
  
  tmp <- bind_rows(general.3, th.1)               
  
##  ............................................................................
##  DEFINIR ORDEN DE COLUMNAS ARCHIVO FINAL                                 ####
  
  orden_cols <- c('ticketid', 'description', 'reportdate',
                  'changedate', 'internalpriority', 'siteid', 'ownergroup',
                  'decimal_duration', 'new_decimal_duration', 'mttr_valido_ts',
                  'empresa', 'sede', 'area', 'target', 'region', 'rca_ts',
                  'ttk_tigo_star')
  

##  ............................................................................
##  FUSIONAR                                                                ####

  
  # fusionar todo y agregar la info de los grupos.        
  maestro  <-      tmp %>%
                   mutate(new_decimal_duration = decimal_duration) %>%
                   arrange(reportdate) %>%
                   left_join(info.grupos, by = 'ownergroup') %>%
                   mutate(rca_ts = NA, ttk_tigo_star = NA) %>%
                   select(orden_cols) %>%
                   mutate_if(is.character, toupper) %>%
                   mutate_at('description', str_sub, star = 1, end = 40) %>%
                   mutate_at(vars(reportdate, changedate), as.character)
                        
  
  # borrar el resto de df
  rm(list = setdiff(ls(), 'maestro'))
  
  
  # abrir conexion 
  con       <-  odbcConnect(dsn = 'yunkel',
                            uid = 'wchavarria',
                            pwd = 'Tigo1234.')
  
  
  varspec <- c('int',             # ticketid
               'varchar(40)',    # description
               'datetime2(0)',    # reportdate
               'datetime2(0)',    # changedate
               'int',             # internalpriority
               'varchar(20)',     # siteid
               'varchar(20)',     # ownergroup
               'float',           # decimal_duration
               'float',           # new_decimal_duration
               'varchar(20)',     # mttr_valido_ts
               'varchar(20)',     # empresa
               'varchar(20)',     # sede
               'varchar(20)',     # area
               'float',           # target
               'varchar(20)',     # region
               'varchar(20)',     # rca_ts
               'varchar(20)')     # ttk_tigo_star
  
  
  names(varspec) <- names(maestro)
  
  # guardar la primera vez con append = FALSE
  sqlSave(channel   = con,
          dat       = maestro,
          rownames  = 'tkstatusid', 
          tablename = 'MTTR',
          append    = FALSE, 
          addPK     = TRUE,
          varTypes  = varspec,
          fast      = TRUE) 


  # borrar tabla
  sqlDrop(channel = con, sqtable = 'MTTR', errors = FALSE)
  
  
  
  
  
  
  # close con
  close(con)



        
        
        