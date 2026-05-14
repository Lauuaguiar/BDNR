# ==============================================================================
# DASHBOARD LOGÍSTICO PRO V6 - SOLUCIÓN TOTAL
# ==============================================================================

library(shiny)
library(shinydashboard)
library(jsonlite)
library(ggplot2)

# 1. CARGA Y PRE-PROCESAMIENTO DE DATOS (R BASE PARA EVITAR ERRORES)
# ------------------------------------------------------------------------------
raw_data <- fromJSON("datos_limpios.json", flatten = TRUE)
df <- as.data.frame(raw_data)

# Limpieza de nombres de columnas (Quita puntos y espacios)
names(df) <- make.names(names(df))

# Función para limpiar cada columna de forma segura
limpiar_columna <- function(x) {
  if(is.list(x)) x <- unlist(lapply(x, function(y) if(is.null(y)) NA else as.character(y)))
  return(as.character(x))
}

# Aplicamos limpieza a las columnas clave detectadas en tu JSON
df$marca <- limpiar_columna(df$market_brand_1)
df$ubicacion <- limpiar_columna(df$cliente_ubicacion)
df$precio <- as.numeric(as.character(df$precio))
df$m_precio <- as.numeric(as.character(df$market_price_1))
df$fecha <- as.Date(as.character(df$fecha))

# Eliminamos filas que rompen las gráficas
df <- df[!is.na(df$precio), ]

# 2. INTERFAZ DE USUARIO (UI)
# ------------------------------------------------------------------------------
ui <- dashboardPage(
  skin = "black",
  dashboardHeader(title = "Logística Intelligence"),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Dashboard Comercial", tabName = "tab_comercial", icon = icon("chart-line")),
      menuItem("Simulador Cassandra", tabName = "tab_cass", icon = icon("database")),
      hr(),
      selectInput("f_marca", "Filtrar Marca:", choices = c("Todas", unique(df$marca)))
    )
  ),
  dashboardBody(
    tabItems(
      # PESTAÑA PRINCIPAL
      tabItem(tabName = "tab_comercial",
        fluidRow(
          box(title = "Tendencia de Ventas", status = "primary", solidHeader = T, 
              plotOutput("plot_trend"), width = 8),
          box(title = "Top 5 Marcas", status = "warning", solidHeader = T, 
              plotOutput("plot_brands"), width = 4)
        ),
        fluidRow(
          box(title = "Mapa de Ubicaciones (Top 10 Ciudades)", status = "info", solidHeader = T, 
              plotOutput("plot_cities"), width = 12)
        )
      ),
      # PESTAÑA CASSANDRA
      tabItem(tabName = "tab_cass",
        fluidRow(
          box(width = 4, status = "danger", title = "Configuración NoSQL",
              sliderInput("nodes", "Nodos del Clúster:", 2, 5, 3),
              p("Simulación de distribución de carga en Cassandra.")),
          box(width = 8, status = "primary", title = "Distribución de Datos", 
              plotOutput("plot_nodes"))
        )
      )
    )
  )
)

# 3. SERVIDOR (SERVER)
# ------------------------------------------------------------------------------
server <- function(input, output) {
  
  # Filtro reactivo simple
  data_r <- reactive({
    res <- df
    if (input$f_marca != "Todas") res <- res[res$marca == input$f_marca, ]
    res
  })

  # 1. TENDENCIA (Línea)
  output$plot_trend <- renderPlot({
    df_t <- as.data.frame(table(data_r()$fecha))
    if(nrow(df_t) == 0) return(NULL)
    names(df_t) <- c("fecha", "n")
    ggplot(df_t, aes(x = as.Date(fecha), y = n)) + 
      geom_line(color = "#3c8dbc", size = 1) + geom_point() +
      theme_minimal() + labs(x = "Fecha", y = "Órdenes")
  })

  # 2. TOP MARCAS (Barras) - CORREGIDA
  output$plot_brands <- renderPlot({
    df_b <- as.data.frame(table(data_r()$marca))
    if(nrow(df_b) == 0) return(NULL)
    names(df_b) <- c("marca", "n")
    df_b <- df_b[order(-df_b$n), ][1:5, ] # Top 5
    
    ggplot(df_b, aes(x = reorder(marca, n), y = n, fill = marca)) + 
      geom_bar(stat = "identity") + coord_flip() +
      theme_minimal() + guides(fill = "none") + labs(x = "", y = "Total")
  })

  # 3. MAPA DE UBICACIONES (Ciudades) - CORREGIDA
  output$plot_cities <- renderPlot({
    df_c <- as.data.frame(table(data_r()$ubicacion))
    if(nrow(df_c) == 0) return(NULL)
    names(df_c) <- c("ciudad", "n")
    df_c <- df_c[order(-df_c$n), ][1:10, ] # Top 10
    
    ggplot(df_c, aes(x = reorder(ciudad, n), y = n)) + 
      geom_col(fill = "#00c0ef") + 
      theme_minimal() + labs(x = "Ciudad", y = "Cantidad de Pedidos")
  })

  # 4. SIMULADOR CASSANDRA
  output$plot_nodes <- renderPlot({
    df_node <- data_r()
    # Asignación de nodo forzada para que las barras existan siempre
    df_node$nodo <- (seq_len(nrow(df_node)) %% input$nodes) + 1
    
    ggplot(df_node, aes(x = as.factor(nodo), fill = as.factor(nodo))) + 
      geom_bar() + theme_minimal() + 
      scale_fill_brewer(palette = "Blues") +
      labs(x = "Nodo Físico", y = "Registros Guardados") + guides(fill = "none")
  })
}

shinyApp(ui, server)