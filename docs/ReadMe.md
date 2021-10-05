---
pagetitle: "ReadMe"
---
# White Rabbit Documentation Readme
This folder contains the raw and rendered documentation of WhiteRabbit.

## Contribute
Contributions to the documentation are very welcome and even a must when new features are implemented.
To update the documentation, edit one of the following markdown files or create a new markdown file:
 - [WhiteRabbit.md](/docs/WhiteRabbit.md)
 - [RabbitInAHat.md](/docs/RabbitInAHat.md)
 - [riah_test_framework.md](/docs/riah_test_framework.md)
 - [best_practices.md](/docs/best_practices.md) 

## Render html
To generate the site from markdown files, run the following R script with the `./docs` folder as working directory.
Run this in a standalone R session, this is orders of magnitude faster compared to running in RStudio.

```R
#devtools::install_github("ropenscilabs/icon")
library(rmarkdown)
rmarkdown::render_site()
```
