require(plyr)
require(ggplot2)
require(Rmisc)
require(nortest)

metrics = function(directory, separator) {
  
  dados_0 = read_data(directory, 0, separator)
  dados_1 = read_data(directory, 1, separator)
  dados_2 = read_data(directory, 2, separator)
  dados_3 = read_data(directory, 3, separator)
  
  dados = rbind(dados_0, dados_1, dados_2, dados_3)
  
  grouped = ddply(dados, .(hour,minute,second), summarize, latency = sum(latency))
  
  return(grouped)
  
}

read_data = function(directory, number, separator) {
  dados_1 = read.csv(paste(directory,paste("worker-trimmed",number,"1.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  dados_2 = read.csv(paste(directory,paste("worker-trimmed",number,"2.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  dados_3 = read.csv(paste(directory,paste("worker-trimmed",number,"3.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  dados_4 = read.csv(paste(directory,paste("worker-trimmed",number,"4.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  dados_5 = read.csv(paste(directory,paste("worker-trimmed",number,"5.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  dados_6 = read.csv(paste(directory,paste("worker-trimmed",number,"6.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  dados_7 = read.csv(paste(directory,paste("worker-trimmed",number,"7.log",sep="-"), sep="/"), sep=separator, header = TRUE)
  
  dados_trimmed_1 = ddply(dados_1, .(hour, minute, second), summarize, latency=sum(latency))
  dados_trimmed_2 = ddply(dados_2, .(hour, minute, second), summarize, latency=sum(latency))
  dados_trimmed_3 = ddply(dados_3, .(hour, minute, second), summarize, latency=sum(latency))
  dados_trimmed_4 = ddply(dados_4, .(hour, minute, second), summarize, latency=sum(latency))
  dados_trimmed_5 = ddply(dados_5, .(hour, minute, second), summarize, latency=sum(latency))
  dados_trimmed_6 = ddply(dados_6, .(hour, minute, second), summarize, latency=sum(latency))
  dados_trimmed_7 = ddply(dados_7, .(hour, minute, second), summarize, latency=sum(latency))
  
  dados_trimmed = rbind(dados_trimmed_1, dados_trimmed_2, dados_trimmed_3, dados_trimmed_4, dados_trimmed_5, dados_trimmed_6, dados_trimmed_7)
  return(dados_trimmed)
}

get_ack_df = function(dff) {
  ack = subset(dff, event=="AckSent",row.names = FALSE)
  return(ack)
}

trim_df = function(dff) {
  trim = dff[1:length(dff$total)-1,]
  trim = remove_last_minute(remove_first_minute(group_df_by_1_minute(trim)))
  return(trim)
}

group_df_by_1_minute = function(dff) {
  count=0
  group=0
  for(row in 1:length(dff$latency)) {
    count = count + 1
    dff$By1[row] = as.numeric(group)
    if(count == 60) {
      group = group + 1 
      count = 0
    }
  }
  return(dff)
}

remove_first_minute = function(dff){
  new_df = subset(dff, By1 != 0)
  return(new_df)
}

remove_last_minute = function(dff){
  new_df = subset(dff, By1 != 3)
  return(new_df)
}

analysis = function(df_ack, num_messages, dff) {
  data_ack = data.frame(latency=mean(df_ack$latency))
  
  data_ack$MessagesPerSecond = num_messages
  
  dff <- rbind(dff, data_ack)
  return(dff)
}
create_frame = function(root_path, list_dados, messages) {
  analysis_final = data.frame(MessagesPerSecond=numeric())
  for (dados in list_dados) {
    dados_ack = trim_df(dados)
    analysis_final = analysis(dados_ack, messages, analysis_final)
  }
  return(analysis_final)
}

create_data_list_by_input_rate = function(rate, root_path) {
  data_0 = metrics(paste(root_path,rate, 0 ,sep="/"), ";")
  data_1 = metrics(paste(root_path,rate, 1 ,sep="/"), ";")
  data_2 = metrics(paste(root_path,rate, 2 ,sep="/"), ";")  
  data_3 = metrics(paste(root_path,rate, 3 ,sep="/"), ";")
  data_4 = metrics(paste(root_path,rate, 4 ,sep="/"), ";")
  data_5 = metrics(paste(root_path,rate, 5 ,sep="/"), ";")
  data_6 = metrics(paste(root_path,rate, 6 ,sep="/"), ";")
  data_7 = metrics(paste(root_path,rate, 7 ,sep="/"), ";")
  data_8 = metrics(paste(root_path,rate, 8 ,sep="/"), ";")  
  data_9 = metrics(paste(root_path,rate, 9 ,sep="/"), ";")  
  
  list_data = list(data_0, data_1, data_2, data_3, data_4, data_5, data_6, data_7, data_8, data_9)
  return(list_data)
}

run = function() {
  root_path = "/local/storm/experimentos/latency/"
  
  list_data_1260 = create_data_list_by_input_rate(1260,root_path)
  list_data_8400 = create_data_list_by_input_rate(8400, root_path)
  list_data_10500 = create_data_list_by_input_rate(10500, root_path)
  list_data_16800 = create_data_list_by_input_rate(16800, root_path)
  
  analysis_table_1260= create_frame(root_path, list_data_1260, 1260)
  analysis_table_8400= create_frame(root_path, list_data_8400, 8400)
  analysis_table_10500= create_frame(root_path, list_data_10500, 10500)
  analysis_table_16800= create_frame(root_path, list_data_16800, 16800)
  
  grouped_analysis = rbind(analysis_table_1260,
                           analysis_table_8400,
                           analysis_table_10500,
                           analysis_table_16800)
  
#   final_table = ddply(grouped_analysis, 
#                       .(MessagesPerSecond), 
#                       summarize, 
#                       latency_mean=mean(latency), 
#                       latency_sd=sd(latency),
#                       latency_min=CI(latency)[3],
#                       latency_max=CI(latency)[1])
  
  return(grouped_analysis)
}  

data = run()
data_ic = ddply(grouped_analysis, 
                .(MessagesPerSecond), 
                summarize, 
                latency_mean=mean(latency), 
                latency_sd=sd(latency),
                latency_min=CI(latency)[3],
                latency_max=CI(latency)[1])

data$index = seq_along(data$MessagesPerSecond)

###Plot

data_1260 = subset(data, data$MessagesPerSecond == 1260)$latency
data_8400 = subset(data, data$MessagesPerSecond == 8400)$latency
data_10500 = subset(data, data$MessagesPerSecond == 10500)$latency
data_16800 = subset(data, data$MessagesPerSecond == 16800)$latency

p1 = qplot(sample = data_1260,  stat = "qq") + ggtitle(expression("1260 Mensagens"))
p2 = qplot(sample = data_8400,  stat = "qq") + ggtitle(expression("8400 Mensagens"))
p3 = qplot(sample = data_10500,  stat = "qq") + ggtitle(expression("10500 Mensagens"))
p4 = qplot(sample = data_16800,  stat = "qq") + ggtitle(expression("16800 Mensagens"))

multiplot(p1, p2, p3, p4, cols=3)

ad.test(data_1260)
ad.test(data_8400)
ad.test(data_10500)
ad.test(data_16800)


png("latency.png")
p <- ggplot() + 
  geom_jitter(data=data, aes(x=MessagesPerSecond, y=latency, colour=as.character(MessagesPerSecond)), alpha=0.3, size=6) +
  geom_errorbar(data=data_ic, aes(x=MessagesPerSecond, ymin=latency_min, ymax=latency_max), width=500, col="red") +
  xlab("Messages/s") +
  ylab("Latency in ms") + theme(legend.position="none")

print(p)
dev.off()

p <- ggplot() + 
  geom_jitter(data=data, aes(x=MessagesPerSecond, y=latency, color=as.character(MessagesPerSecond)), alpha=0.3, size=6) +
  geom_errorbar(data=data_ic, aes(x=MessagesPerSecond, ymin=latency_min, ymax=latency_max), width=500, col="red") +
  xlab("Messages/s") +
  ylab("Latency in ms") + theme(legend.position="none") +
  scale_y_continuous(limits = c(0,30)) 

print(p)

# ggplot(data_0, aes(x=MessagesPerSecond, y=Ack_Mean, colour="red")) + 
#   geom_errorbar(aes(ymin=Ack_Mean-(Ack_SD*1.96), ymax=Ack_Mean+(Ack_SD*1.96), width=1000) +
#   geom_point()
