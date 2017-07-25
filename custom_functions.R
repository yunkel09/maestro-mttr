#   ____________________________________________________________________________
#   CUSTOM FUNCTIONS FOR MTTR PROJECT                                       ####


hms_to_decimal <- function(vec){
      y <- sapply(strsplit(vec, ':'),
                function(x) { x <- as.numeric(x); x[1] + x[2]/60 + x[3] / 3600})
      return(round(y,2))
}


## validar integridad del dataframe

validar_df <- function(df){
        
# validaciones      
        
## tiene registros
  if(nrow(df) == 0){
    return(FALSE)
  } 
        
  ## tkstatusid es un numero entero
  if(!is.integer(df$tkstatusid)){
     stop('Los valores de tkstatusid deben de ser numeros enteros',
          call. = FALSE)
  }
        
  ## tkstatusid tiene 8 caracteres
  if(any(nchar(df$tkstatusid) != 8)){
                
  index <- which(sapply(df$tkstatusid, function(x) nchar(x) != 8))
  l     <- df$tkstatusid[index]
  cat('Los siguientes registros estan incorrectos:\n', l,
      '\ntienen longitud diferente de 8 caracteres\n')
  stop('la regaste, revisa bien', call. = FALSE)
  }
        
  ## la columna tkstatusid no tiene duplicados
  if(any(duplicated(df$tkstatusid))){
        my.id <- which(duplicated(df$tkstatusid))
        cat('Los siguientes registros están duplicados:\n', df$tkstatusid[my.id])
        stop('Hay tkstatusid que estan duplicados', call. = FALSE)
  }
        
  ## la columna duracion decimal es numerica
  if(!is.numeric(df$duracion_decimal)){
        stop('La columna duracion decimal no es numerica, revisa bien por favor',
             call. = FALSE )
  }
        
        
  ## regresar TRUE si todo esta bien
  return(TRUE)  
}
        
#   ____________________________________________________________________________
#   Parsear fechas                                                          ####

parsear_fechas <- function(x){
        x <- x %>% str_sub(start = 1, end = 19) %>%
                   str_replace_all(pattern = 'T', replacement = " ") %>%
                   ymd_hms(quiet = TRUE, tz = 'America/Guatemala')
        return(x)
}


#   ____________________________________________________________________________
#   Corregir fechas en archivo BUC                                          ####

corregir_fecha <- function(fecha) {
        mes     <- str_sub(fecha, start = 1, end = 3)
        dia     <- str_sub(fecha, start = 5, end = 6)
        hora    <- str_sub(fecha, start = 14, end = 21)
        myyear  <- rep.int(x = '2016', times = length(fecha))
        fecha   <- str_c(dia,mes, myyear, hora, sep = ' ')
        fecha   %<>% dmy_hm(tz = 'America/Guatemala')
        return(fecha)
}




