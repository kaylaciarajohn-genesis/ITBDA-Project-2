# Load required libraries
library(rvest)
library(dplyr)
library(stringr)
library(xml2)

# Store the webpage URL
url <- "https://science.nasa.gov/earth/earth-observatory/watching-the-world-rev-its-heat-engine-2152/"

# Read the webpage
webpage <- read_html(url)

# Extract the article title
article_title <- webpage %>%
  html_element("h1") %>%
  html_text2()

# Extract the article paragraphs
article_content <- webpage %>%
  html_elements("article p") %>%
  html_text2()

# Display the title
print(article_title)

# Display the extracted paragraphs
print(article_content)

