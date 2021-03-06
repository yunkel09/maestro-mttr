#   ____________________________________________________________________________
#   MTTR                                                                    ####

  # cargar librerias
  pkgs  <- c('tidyverse',
             'RODBC',
             'magrittr',
             'data.table',
             'lubridate',
             'stringr')
  
  sapply(pkgs, require, character.only = TRUE)
  
  # cargar funciones
  source('custom_functions.R')

  # abrir conexion 
  con       <-  odbcConnect(dsn = 'yunkel',
                            uid = 'wchavarria',
                            pwd = 'Tigo1234.')

  # leer tabla incidentes
  incidents <-  sqlQuery(channel    = con, 
                         query      =   'SELECT
                                         DESCRIPTION,
                                         INTERNALPRIORITY,
                                         REPORTDATE,
                                         TICKETID,
                                         REPORTEDPRIORITY,
                                         INDICATEDPRIORITY,
                                         ISGLOBAL,
                                         RELATEDTOGLOBAL
                                         FROM TIVOLI.INCIDENT',
                         na.strings = '',
                         stringsAsFactors = FALSE)
  
  
  # leer tabla ttks
  ttk       <-  sqlQuery(channel = con, 
                         query   = 'SELECT
                                      TKSTATUSID,
                                      OWNERGROUP,
                                      SITEID,
                                      STATUSTRACKING,
                                      TICKETID,
                                      STATUS,                
                                      CHANGEDATE
                                    FROM TIVOLI.TKSTATUS',
                         na.strings = '',
                         stringsAsFactors = FALSE)
  
  # cambiar a minuscula nombres de columnas
  names(incidents)  %<>% tolower
  names(ttk)        %<>% tolower

  # definir el orden final de las columnas
  colOrder              <- c('tkstatusid',
                             'ticketid',
                             'description',
                             'reportdate',
                             'changedate',
                             'internalpriority',
                             'siteid',
                             'ownergroup',
                             'decimal_duration',
                             'reportedpriority',
                             'indicatedpriority',
                             'isglobal',
                             'relatedtoglobal')

 # transformar incidentes modificando formatos de fechas
 incidents.1 <- incidents %>%
                as_tibble() %>%
                mutate_at('reportdate', parsear_fechas) 
 
 # transformar tabla ttks modificando formato de fechas
 ttk.1 <-      ttk %>%
               as_tibble() %>%
               mutate_at('changedate', parsear_fechas)
 
 # crear tabla maestra
 mttr.1 <-      ttk.1 %>%
                left_join(incidents.1, by = 'ticketid') %>%
                mutate(decimal_duration = hms_to_decimal(statustracking)) %>%
                filter(decimal_duration > 0,
                       !is.na(decimal_duration),
                       status == 'INPROG',
                       !is.na(ownergroup)) %>%
                select(colOrder) %>%
                mutate_at('description', str_sub, star = 1, end = 40) %>%
                mutate_at('description', toupper) %>%
                arrange(reportdate)

##  ............................................................................
##  FILTRADO POR OWNERGROUPS VALIDOS                                        ####

 # cargar catalogo de ownergroups validos
  # ownergroups <- fread(input      = './files/01.- valid_ownergroups.csv',
  #                      data.table = FALSE)
 
 ownergroups     <- sqlQuery(channel = con, 
                        query   = 'SELECT
                        OWNERGROUP
                        FROM shd.contratista_owner_group WHERE ACTIVO = 1',
                        na.strings = '',
                        stringsAsFactors = FALSE)
 
 
 # dejar solo los ownergroups que estan en el listado
  mttr.2 <- mttr.1 %>%
            filter(mttr.1$ownergroup %in% ownergroups$include)

##  ............................................................................
##  EXCLUIR REGISTROS QUE NO APLIQUEN                                       ####

 # leer listado de registros a excluir
  exclusion <- fread(input      = './files/02.- registros_excluidos.csv',
                     data.table = FALSE,
                     select     = 'tkstatusid')
 
 # validacion: si no hay registros que excluir, copiar igual
 if(nrow(exclusion) == 0) {
          mttr.3 <- mttr.2

  } else if(!is.integer(exclusion$tkstatusid)) {
                        stop('Los valores de tkstatusid deben de
                             ser numeros enteros',
                             call. = FALSE)
         } else {

                  # de lo contrario excluir esos registros
                  mttr.3 <- mttr.2 %>%
                            filter(!mttr.2$tkstatusid %in% exclusion$tkstatusid)

                }

##  ............................................................................
##  MODIFICAR LA DURACION DE REGISTROS                                      ####

  # leer listado de registros con nueva duracion
  duracion <- fread(input      = './files/03.- actualizar_duracion.csv',
                    data.table = FALSE)
  
  # validar df
  #' Si la funcion validar_df retorna FALSE, entonces solo se copia el mismo
  #' df mttr.3 y se le agrega la columna new_decimal_duration como una copia
  #' de la columna 'decimal_duration'.  Si la funcion retorna TRUE, entonces
  #' se agrega la misma columna, pero busca los tkstatusid y coloca las
  #' duraciones modificadas en la columna new_decimal duration.
  
  if(!validar_df(duracion)){
    mttr.4 <- mttr.3 %>%
              mutate(new_decimal_duration = decimal_duration)
    
  } else {
    mttr.4 <- mttr.3 %>%
              mutate(new_decimal_duration =
                     ifelse(test = mttr.3$tkstatusid %in% duracion$tkstatusid,
                            yes  = duracion$duracion_decimal,
                            no   = mttr.3$decimal_duration))
  }  
        
          
##  ............................................................................
##  PROCESAR LA INFORMACION DE FAILURE REPORT PARA TIGO STAR                ####

 # definir columnas necesarias
 tigo_star.1 <-  sqlQuery(channel     = con, 
                                query = 'SELECT
                                         FAILURE_REPORT.TICKETID,
                                         FAILURE_REPORT.TYPE,
                                         FAILURE_REPORT.DESCRIPTION,
                                         INCIDENT.SITEID
                                         FROM TIVOLI.FAILURE_REPORT
                                         JOIN TIVOLI.INCIDENT ON
                                         TIVOLI.FAILURE_REPORT.TICKETID =
                                         TIVOLI.INCIDENT.TICKETID',
                            na.strings = '',
                      stringsAsFactors = FALSE)
 
 names(tigo_star.1) %<>% tolower
 
 # procesar tabla
 tigo_star.2 <- tigo_star.1 %>%
                filter(siteid == "TH",
                       type    == "CAUSE",
                       !is.na(description)) %>%
                mutate_at('description', toupper) %>%
                mutate(ttk_tigo_star = "NO APLICA") %>%
                select(-type, -siteid) %>%
                rename(rca_ts = description) %>%
                mutate_at('rca_ts', str_sub, star = 1, end = 40)
  
  
  # leer tabla de categorias invalidas de rca tigo star
  rm.ts <-      fread(input      = './files/05.- remove_category_th.csv',
                      data.table = FALSE,
                      select     = 'categoria')
        
  # Remover categorias
  tigo_star.3 <- tigo_star.2 %>%
                 filter(!(tigo_star.2$rca_ts %in% rm.ts$categoria))

### . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ..
### DEFINIR ORDEN FINAL DE COLUMNAS                                         ####
  
  FinalOrder            <- c('tkstatusid',
                             'ticketid',
                             'description',
                             'reportdate',
                             'changedate',
                             'internalpriority',
                             'siteid',
                             'ownergroup',
                             'decimal_duration',
                             'new_decimal_duration',
                             'empresa',
                             'sede',
                             'area',
                             'target',
                             'region',
                             'rca_ts',
                             'ttk_tigo_star',
                             'reportedpriority',
                             'indicatedpriority',
                             'isglobal',
                             'relatedtoglobal')
  
##  ............................................................................
##  AGREGAR INFORMACION OWNERGROUPS Y TIGO STAR                             ####

 # cargar tabla lookup de ownergroups que deben compararse
 info.grupos <- fread(input      = './files/06.- info_grupos.csv',
                      data.table = FALSE)
 
 # fusionar todo
 mttr.5 <-      mttr.4 %>%
                left_join(info.grupos, by = 'ownergroup') %>%
                left_join(tigo_star.3, by = 'ticketid')   %>%
                select(FinalOrder)
 
##  ............................................................................
##  PREPARAR Y GUARDAR TABLA EN BD                                          ####

 # modificar los formatos de fecha para que sean texto       
 mttr.6 <- mttr.5 %>% mutate_if(is.POSIXct, as.character)

 # cerrar conexion
 close(con)
 
 # borrar objetos innecesarios                  
 rm(list = setdiff(ls(), 'mttr.6'))
 
 

