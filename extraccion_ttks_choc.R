#   ____________________________________________________________________________
#   ARCHIVO MAESTRO CHOC A BD                                               ####

  # paquetes necesarios
  paquetes <- c('lubridate', 'tidyverse', 'magrittr', 'data.table', 'stringr', 'RODBC')
        
  # cargar librerías
  sapply(paquetes, require, character.only = TRUE, quiet = TRUE)
  
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


  # cargar tabla lookup de ownergroups que deben compararse
  info.grupos <- fread(input      = './files/06.- info_grupos.csv',
                       data.table = FALSE)
  
  
##  ............................................................................
##  TRANSFORMAR ARCHIVO BUC                                                 ####

  

  # fusionar todo        
  maestro       <-      general %>%
                        mutate(tkstatusid = row.names(.),
                               new_decimal_duration = decimal_duration) %>%
                        left_join(info.grupos, by = 'ownergroup') %>%
                        mutate(rca_ts = NA, ttk_tigo_star = NA) %>%
                        arrange(reportdate) %>%
                        mutate_at('tkstatusid', as.integer) %>%
                        select(orden_cols) %>%
                        mutate_if(is.character, toupper) %>%
                        mutate_at('description', str_sub, star = 1, end = 40)
  
  

# borrar el resto de df
rm(list = setdiff(ls(), c('maestro.1', 'maestro')))

# abrir conexion 
con       <-  odbcConnect(dsn = 'yunkel',
                          uid = 'wchavarria',
                          pwd = 'Tigo1234.')



# guardar la primera vez con append = FALSE
sqlSave(channel   = con,
        dat       = maestro,
        tablename = 'MTTR',
        rownames  = FALSE,
        append    = FALSE)



# close con
close(con)



        
        
        