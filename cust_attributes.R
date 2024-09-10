rm(list = ls())

dat <- read.csv("Customer Attributes.csv", header = T)
dat$avg_ord_size <- (dat$net_sales_amt/dat$inv_count)

## AR Percent Current
x <- subset(dat, dat$ar_pct_curr > 0 & dat$ar_pct_curr < 1)
x <- x[order(x$ar_pct_curr), ]
h <- hist(x$ar_pct_curr, breaks=40, plot=F)
cols <- c('grey', "#8DD3C7", "#FFFFB3", "#BEBADA", "#FB8072")  
k <- cols[findInterval(h$mids, quantile(x$ar_pct_curr), rightmost.closed=T, all.inside=F) + 1]
plot(h, col=k, main=paste("Histogram of AR% Current"))

## Invoice Count
x <- dat[order(dat$inv_count), ]
x <- subset(x, scale(x$inv_count) > -4 & scale(x$inv_count) < 4)

h <- hist(x$inv_count, breaks=50, plot=F)
cols <- c('grey', "#8DD3C7", "#FFFFB3", "#BEBADA", "#FB8072")  
k <- cols[findInterval(h$mids, quantile(x$inv_count), rightmost.closed=T, all.inside=F) + 1]
plot(h, col=k, main=paste("Histogram of Total Invoice Count"))





dat <- subset(dat, scale(dat$days_on_hand) > -4 &
                scale(fill_rate) > -4 &
                scale(on_time_rate) > -4 &
                scale(service_level) > -4 &
                scale(wmape) > -4 &
                scale(days_on_hand) < 4 & 
                scale(fill_rate) < 4 &
                scale(on_time_rate) < 4 &
                scale(service_level) < 4 &
                spoilage >= 0 & 
                scale(spoilage) < 4 &
                scale(wmape) < 4)
