library('tidyverse')

library('urbnmapr')

stateData <- read_csv("~/DataScience/incarceration/data/ratesByStateAndYear.csv")

stateData2014 = subset(stateData,year==2014)

longStateData2014 = reshape2::melt(stateData2014,id.vars=c("state","year"))

ggplot(left_join(longStateData2014,states,by = c("state"="state_abbv"))) + geom_polygon(mapping=aes(long,lat,group=group,fill=value),color = "#ffffff", size = .25) + coord_map(projection="albers", lat0 = 39, lat1 = 45) + facet_grid(rows=vars(variable)) + scale_fill_continuous(limits=c(0,0.2))

# plot is of long and lat where the group is what puts the correct data in each state
# also, note the left_join to join state level data to the state geometry.
# we melt the data to get the rates in same col
# we limit the scale because Alaska has a 50% (!!) incarceration rate and that undoes all the colors

