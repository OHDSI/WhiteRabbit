#!/usr/bin/env Rscript

#
# use this script (on linux/mac) to reformat the html pages from the markdown files
# only needed when you change .md files
#

#devtools::install_github("ropenscilabs/icon")
library(rmarkdown)
rmarkdown::render_site()
