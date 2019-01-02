Vera Incarceration Trends notes
================
Adam Conner-Sax

Data was processed from the Vera incarceration trends data-set (<https://github.com/vera-institute/incarceration_trends>) and processed (aggregations, filtering, dropping of missing values) byt the author ()

Aggregated crime rate and imprisoned per crime by urbanicity:

![](notes_files/figure-markdown_github/urbanicity-1.png)

Combo with poverty/income data

``` r
trendsWithIncome <- read_csv(here("../../data/", "scatterMergeTest.csv"))
#trends1995 = filter(trendsWithIncome,year==2013)
ggplot(trendsWithIncome,mapping=aes(x=medianHI,y=IncarcerationRate,color=total_pop)) + geom_point() + facet_wrap(~year,ncol=3) + scale_y_continuous(limits=c(NA,0.012), labels = scales::percent)
```

    ## Warning: Removed 20 rows containing missing values (geom_point).

![](notes_files/figure-markdown_github/income%20scatter-1.png)

``` r
ggsave("incarcerationRateAndIncome.pdf",width=8,height=11)
```

    ## Warning: Removed 20 rows containing missing values (geom_point).

From call
=========

Bail reform, CO have data from open records requests (What data? Just CO?) clickable map of CO counties census cdc reform policy by location
