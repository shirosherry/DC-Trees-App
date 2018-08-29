library(leaflet)
library(RColorBrewer)
library(dplyr)

alltrees <- read.csv("DC_trees.csv")
alltrees$latitude <- jitter(alltrees$latitude)
alltrees$longitude <- jitter(alltrees$longitude)
row.names(alltrees) <- alltrees$OBJECTID

# Leaflet bindings are a bit slow; for now we'll just sample to compensate
set.seed(100)
sptrees <- alltrees[sample.int(nrow(alltrees), 10000),]
sptrees <- sptrees[order(sptrees$CMMN_NM),]
vars <- c('All',levels(sptrees$CMMN_NM))

# Define UI for application that draws a histogram
ui <- navbarPage("DC Trees", id="nav",
             
             tabPanel("Interactive map",
                      div(class="outer",
                          
                          tags$head(
                            includeCSS("styles.css")
                          ),
                          
                          leafletOutput("map", width="100%", height="100%"),
                          
                          absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                                        draggable = TRUE, top = 60, left = "auto", right = 20, bottom = "auto",
                                        width = 430, height = "auto",
                                        
                                        h2("Tree Explorer"),
                                        
                                        selectInput("sector", "Common Name", vars, selected = 'All'),

                                        
                                        plotOutput("histCMMN_NM"),
                                        h5("The 1st Version of My DC Street Trees App"),
                                        tags$p('I may upgrade it in the comming winter holiday :-)'),
                                        br(),
                                        p('I learnt a great deal from the ', a(herf='https://shiny.rstudio.com/gallery/superzip-example.html',target='"_blank"',
                                            'SuperZip example'),' , thanks to the Rshiny team.')
                          ),
                          
                          tags$div(id="cite",
                                   'Data source: ', a(herf='http://opendata.dc.gov/datasets/f6c3c04113944f23a7993f2e603abaf2_23',target='"_blank"',
				tags$em('DCGIS Open Data: Environment - Urban Forestry Street Trees')), 'shared by DCGISopendata.'
                          )
                      )
             ),
             
             
             conditionalPanel("false", icon("crosshair"))
)


server <- function(input, output, session) {
  
  # Create the map
  output$map <- renderLeaflet({
    leaflet() %>%
      addTiles(
        urlTemplate = "//{s}.tiles.mapbox.com/v3/jcheng.map-5ebohr46/{z}/{x}/{y}.png",
        attribution = 'Maps by <a href="http://www.mapbox.com/">Mapbox</a>'
      ) %>%
      setView(lng = -77.01, lat = 38.91, zoom = 13)
  })
  
  # A reactive expression that returns the set of tree locations that are in bounds right now
  zipsInBounds <- reactive({
    if (is.null(input$map_bounds))
      return(sptrees[FALSE,])
    bounds <- input$map_bounds
    latRng <- range(bounds$north, bounds$south)
    lngRng <- range(bounds$east, bounds$west)
    
    subset(sptrees,
           latitude >= latRng[1] & latitude <= latRng[2] &
             longitude >= lngRng[1] & longitude <= lngRng[2])
  })
  
  
  output$histCMMN_NM <- renderPlot({
    # If no OBJECTIDs are in view, don't plot
    if (nrow(zipsInBounds()) == 0)
      return(NULL)
    
    barplot(table(zipsInBounds()$FAM_NAME),
         main = "Tree Family Distribution",
         ylab = "Family Name",
         horiz = TRUE,
         col = '#008833',
         border = 'white')
  })

  observe({
    selector <- input$sector
    if (selector == "All") {
      treedata <- sptrees
    } else {
      treedata <- sptrees[sptrees[,'CMMN_NM']==selector,]
    }
      colorData <- treedata[['FAM_NAME']]
      pal <- colorFactor(rainbow(35,s=0.95,v=0.85), colorData, ordered = TRUE)
      radius <- 50    
    leafletProxy("map", data = treedata) %>%
      clearShapes() %>%
      addCircles(~longitude, ~latitude, radius=radius, layerId=~OBJECTID,
                 stroke=FALSE, fillOpacity=0.6, fillColor=pal(colorData)) %>%
      addLegend("bottomleft", pal=pal, values=colorData, title='Family Name',layerId="colorLegend")
  })
  
  # Show a popup at the given location
  showOBJECTIDPopup <- function(eventid, lat, lng) {
    selectedZip <- alltrees[alltrees$OBJECTID == eventid,]
    content <- as.character(tagList(
      tags$h4(selectedZip$CMMN_NM),
      tags$strong(HTML(sprintf('%s',selectedZip$VICINITY))), tags$br(),
      sprintf("Scientific Name: %s", selectedZip$SCI_NM), tags$br(),
      sprintf("Family Name: %s", selectedZip$FAM_NAME), tags$br(),
      sprintf("Genus Name: %s", selectedZip$GENUS_NAME)
    ))
    leafletProxy("map") %>% addPopups(lng, lat, content, layerId = eventid)
  }
  
  # When map is clicked, show a popup with tree info
  observe({
    leafletProxy("map") %>% clearPopups()
    event <- input$map_shape_click
    if (is.null(event))
      return()
    
    isolate({
      showOBJECTIDPopup(event$id, event$lat, event$lng)
    })
  })
}

# Run the application 
shinyApp(ui = ui, server = server)

