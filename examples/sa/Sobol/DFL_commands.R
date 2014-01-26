# Definition of the function on which the SA analysis is applied (in this case this is an external function with outputs Y)
library("sensitivity")


Mysobol.fun <- function(X) {
	temp <- Y[,Yindex]
	mean <- mean(temp)
        y <- temp-mean
         #y<-temp
}

# Read vectors/array required for SA analysis                 

X1 <- read.table('./SimRes/X1-input.dat')    # re-sample matrix
X2 <- read.table('./SimRes/X2-input.dat')    # sample matrix
Y <- read.csv('./SimRes/Xsim-output.csv')    # outputs of simulator


ninput  <- dim(X1)[2]               # number of input model parameters
noutput <- dim(Y)[2]                # number of output parameters
n       <- 4                        # number of output quantities per output-parameter (i.e. MAIN and TOTAL indices plus their standard deviations)
temp    <- as.data.frame(array(0, c(ninput, n*noutput)))
titles  <- array('', c(n*noutput))

for (i in 1:noutput) {
  Yindex <- i
  Yindex <- 1
    
# We are not able to calculate the confidence intervals and the indices when the output does not change since the indices are Inf (V(Y)-->0 with respect to # VCE amd VCE.compl) or undefined (V(Y)=0). Therefore, a index of NaN is given when 95% of the data is the same.

  if (quantile(Y[,Yindex], c(.05))/quantile(Y[,Yindex],c(0.95)) == 1){
    
    temp[,n*(i-1)+1]  = -10 #'At least 90% of the output data was the same'
    temp[,n*(i-1)+2]  = -10 #'At least 90% of the output data was the same'
    temp[,n*(i-1)+3]  = -10 #'At least 90% of the output data was the same'
    temp[,n*(i-1)+4]  = -10 #'At least 90% of the output data was the same'
    
# -10 in sobol-indices.csv means that at least 90% of the data was the same.
    
    titles[n*(i-1)+1] = paste(colnames(Y)[i], '_MAIN', sep='')
    titles[n*(i-1)+2] = paste(colnames(Y)[i], '_MAIN_stderr', sep='')
    titles[n*(i-1)+3] = paste(colnames(Y)[i], '_TOTAL', sep='')
    titles[n*(i-1)+4] = paste(colnames(Y)[i], '_TOTAL_stderr', sep='')
  }  
    else { 

    x <- sobol2002(model = Mysobol.fun, X1, X2, nboot = 1000)   

    temp[,n*(i-1)+1]  = x$S[,'original']
    temp[,n*(i-1)+2]  = x$S[,'std. error']
    temp[,n*(i-1)+3]  = x$T[,'original']
    temp[,n*(i-1)+4]  = x$T[,'std. error']
    titles[n*(i-1)+1] = paste(colnames(Y)[i], '_MAIN', sep='')
    titles[n*(i-1)+2] = paste(colnames(Y)[i], '_MAIN_stderr', sep='')
    titles[n*(i-1)+3] = paste(colnames(Y)[i], '_TOTAL', sep='')
    titles[n*(i-1)+4] = paste(colnames(Y)[i], '_TOTAL_stderr', sep='')
   }
 }

names(temp) <- titles
write.csv(temp, file='./SimRes/sobol-indices.csv', quote=FALSE, row.names=FALSE)


