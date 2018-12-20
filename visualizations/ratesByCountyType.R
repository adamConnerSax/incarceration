library('tidyverse')
urbanicity <- read.csv("DataScience/incarceration/data/ratesByUrbanicityAndYear.csv")

# all on one
allTogether <- ggplot(data=urbanicity) + geom_line(mapping=aes(x=year,y=CrimeRate,colour=urbanicity)) + geom_line(mapping=aes(x=year,y=ImprisonedPerCrimeRate,colour=urbanicity),linetype=2) + labs(title="Crime (solid) and Imprisoned per Crime (dashed) rates by type of county", y="") + scale_y_continuous(labels=scales::percent)

# faceted
asRows <- ggplot(data=urbanicity) + geom_line(mapping=aes(x=year,y=CrimeRate,colour=urbanicity)) + geom_line(mapping=aes(x=year,y=ImprisonedPerCrimeRate,colour=urbanicity),linetype=2) + labs(title="Crime (solid) and Imprisoned per Crime (dashed) rates by type of county", y="") + scale_y_continuous(labels=scales::percent) + facet_grid(rows=vars(urbanicity))

#ggsave("DataScience/incarceration/visualizations/output/ratesByCountyType.pdf",width=20,height=20,units="cm")