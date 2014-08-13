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
  
  grouped = ddply(dados, .(hour,minute,second,event), summarize,total = sum(total))
  
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
  
  dados_trimmed_1 = ddply(dados_1, .(hour, minute, second, event), summarize, total=sum(total))
  dados_trimmed_2 = ddply(dados_2, .(hour, minute, second, event), summarize, total=sum(total))
  dados_trimmed_3 = ddply(dados_3, .(hour, minute, second, event), summarize, total=sum(total))
  dados_trimmed_4 = ddply(dados_4, .(hour, minute, second, event), summarize, total=sum(total))
  dados_trimmed_5 = ddply(dados_5, .(hour, minute, second, event), summarize, total=sum(total))
  dados_trimmed_6 = ddply(dados_6, .(hour, minute, second, event), summarize, total=sum(total))
  dados_trimmed_7 = ddply(dados_7, .(hour, minute, second, event), summarize, total=sum(total))
  
  dados_trimmed = rbind(dados_trimmed_1, dados_trimmed_2, dados_trimmed_3, dados_trimmed_4, dados_trimmed_5, dados_trimmed_6, dados_trimmed_7)
  return(dados_trimmed)
}

get_event_df = function(dff) {
  event = subset(dff, event=="EventSent", row.names = FALSE)
  return(event)
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

group_df_by_10_minutes = function(dff) {
  count=0
  group=0
  for(row in 1:nrow(dff)) {
    count = count + 1
    dff$By10[row] = group
    if(count == 600) {
      group = group + 1 
      count = 0
    }
  }
  return(dff)
}

group_df_by_1_minute = function(dff) {
  count=0
  group=0
  for(row in 1:length(dff$total)) {
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
  
analysis = function(df_ack, df_event, num_messages, dff) {
  data_event = data.frame(mean_event=mean(df_event$total))
  data_ack = data.frame(mean_ack=mean(df_ack$total))
  
  data_event$MessagesPerSecond = num_messages
  data_ack$MessagesPerSecond = num_messages
  
  new_df = merge(data_ack, data_event, by = c("MessagesPerSecond"))
  dff <- rbind(dff, new_df)
  return(dff)
}

create_frame = function(root_path, list_dados, messages) {
  analysis_final = data.frame(MessagesPerSecond=numeric())
  for (dados in list_dados) {
    dados_event = trim_df(get_event_df(dados))
    dados_ack = trim_df(get_ack_df(dados))
    analysis_final = analysis(dados_ack, dados_event, messages, analysis_final)
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

run = function(type) {
  if(type=="scalability") {
    root_path = "/local/storm/experimentos/scalability2/"
  }else{
    root_path = "/local/storm/experimentos/throughput2/"
  }
  
  print(root_path)
  list_data_420 = create_data_list_by_input_rate(420, root_path)
  list_data_1260 = create_data_list_by_input_rate(1260,root_path)
  list_data_2100 = create_data_list_by_input_rate(2100, root_path)
  list_data_4200 = create_data_list_by_input_rate(4200, root_path)
  list_data_8400 = create_data_list_by_input_rate(8400, root_path)
  list_data_16800 = create_data_list_by_input_rate(16800, root_path)
  list_data_25200 = create_data_list_by_input_rate(25200, root_path)
  list_data_33600 = create_data_list_by_input_rate(33600, root_path)
  
  analysis_table_420= create_frame(root_path, list_data_420, 420)
  analysis_table_1260= create_frame(root_path, list_data_1260, 1260)
  analysis_table_2100= create_frame(root_path, list_data_2100, 2100)
  analysis_table_4200= create_frame(root_path, list_data_4200, 4200)
  analysis_table_8400= create_frame(root_path, list_data_8400, 8400)
  analysis_table_16800= create_frame(root_path, list_data_16800, 16800)
  analysis_table_25200= create_frame(root_path, list_data_25200, 25200)
  analysis_table_33600= create_frame(root_path, list_data_33600, 33600)
  
  grouped_analysis = rbind(analysis_table_420,
                           analysis_table_1260,
                           analysis_table_2100,
                           analysis_table_4200,
                           analysis_table_8400,
                           analysis_table_16800,
                           analysis_table_25200,
                           analysis_table_33600)
  
  return(grouped_analysis)
  
}



scalability = run("scalability")

scalability_grouped = ddply(scalability,
                            .(MessagesPerSecond), 
                            summarize, 
                            ack_mean=mean(mean_ack), 
                            ack_sd=sd(mean_ack),
                            ack_min=CI(mean_ack)[3],
                            ack_max=CI(mean_ack)[1],
                            event_mean=mean(mean_event),
                            event_sd=sd(mean_event),
                            event_min=CI(mean_event)[3],
                            event_max=CI(mean_event)[1])

throughput = run("throughput")

throughput_grouped = ddply(throughput, 
                          .(MessagesPerSecond), 
                          summarize, 
                          ack_mean=mean(mean_ack), 
                          ack_sd=sd(mean_ack),
                          ack_min=CI(mean_ack)[3],
                          ack_max=CI(mean_ack)[1],
                          event_mean=mean(mean_event),
                          event_sd=sd(mean_event),
                          event_min=CI(mean_event)[3],
                          event_max=CI(mean_event)[1])

scalability_grouped$index = seq_along(scalability_grouped$ack_mean)
throughput_grouped$index = seq_along(throughput_grouped$ack_mean)

scalability_grouped$type = "scalability"
throughput_grouped$type = "throughput"

data_final = rbind(scalability_grouped, throughput_grouped)

###Normality Test Scalability

data_scalability_420 = subset(scalability, scalability$MessagesPerSecond == 420)$mean_ack
data_scalability_1260 = subset(scalability, scalability$MessagesPerSecond == 1260)$mean_ack
data_scalability_2100 = subset(scalability, scalability$MessagesPerSecond == 2100)$mean_ack
data_scalability_4200 = subset(scalability, scalability$MessagesPerSecond == 4200)$mean_ack
data_scalability_8400 = subset(scalability, scalability$MessagesPerSecond == 8400)$mean_ack
data_scalability_16800 = subset(scalability, scalability$MessagesPerSecond == 16800)$mean_ack
data_scalability_25200 = subset(scalability, scalability$MessagesPerSecond == 25200)$mean_ack
data_scalability_33600 = subset(scalability, scalability$MessagesPerSecond == 33600)$mean_ack


p1 = qplot(sample = data_scalability_420,  stat = "qq") + ggtitle(expression("420 Mensagens"))
p2 = qplot(sample = data_scalability_1260,  stat = "qq") + ggtitle(expression("1260 Mensagens"))
p3 = qplot(sample = data_scalability_2100,  stat = "qq") + ggtitle(expression("2100 Mensagens"))
p4 = qplot(sample = data_scalability_4200,  stat = "qq") + ggtitle(expression("4200 Mensagens"))
p5 = qplot(sample = data_scalability_8400,  stat = "qq") + ggtitle(expression("8400 Mensagens"))
p6 = qplot(sample = data_scalability_16800,  stat = "qq") + ggtitle(expression("16800 Mensagens"))
p7 = qplot(sample = data_scalability_25200,  stat = "qq") + ggtitle(expression("25200 Mensagens"))
p8 = qplot(sample = data_scalability_33600,  stat = "qq") + ggtitle(expression("33600 Mensagens"))

multiplot(p1, p2, p3, p4, p5, p6, p7, p8, cols=4)

ad.test(data_scalability_420)
ad.test(data_scalability_1260)
ad.test(data_scalability_2100)
ad.test(data_scalability_4200)
ad.test(data_scalability_8400)
ad.test(data_scalability_16800)
ad.test(data_scalability_25200)
ad.test(data_scalability_33600)

###Normality Test Throughput

data_throughput_420 = subset(throughput, throughput$MessagesPerSecond == 420)$mean_ack
data_throughput_1260 = subset(throughput, throughput$MessagesPerSecond == 1260)$mean_ack
data_throughput_2100 = subset(throughput, throughput$MessagesPerSecond == 2100)$mean_ack
data_throughput_4200 = subset(throughput, throughput$MessagesPerSecond == 4200)$mean_ack
data_throughput_8400 = subset(throughput, throughput$MessagesPerSecond == 8400)$mean_ack
data_throughput_16800 = subset(throughput, throughput$MessagesPerSecond == 16800)$mean_ack
data_throughput_25200 = subset(throughput, throughput$MessagesPerSecond == 25200)$mean_ack
data_throughput_33600 = subset(throughput, throughput$MessagesPerSecond == 33600)$mean_ack


p1 = qplot(sample = data_throughput_420,  stat = "qq") + ggtitle(expression("420 Mensagens"))
p2 = qplot(sample = data_throughput_1260,  stat = "qq") + ggtitle(expression("1260 Mensagens"))
p3 = qplot(sample = data_throughput_2100,  stat = "qq") + ggtitle(expression("2100 Mensagens"))
p4 = qplot(sample = data_throughput_4200,  stat = "qq") + ggtitle(expression("4200 Mensagens"))
p5 = qplot(sample = data_throughput_8400,  stat = "qq") + ggtitle(expression("8400 Mensagens"))
p6 = qplot(sample = data_throughput_16800,  stat = "qq") + ggtitle(expression("16800 Mensagens"))
p7 = qplot(sample = data_throughput_25200,  stat = "qq") + ggtitle(expression("25200 Mensagens"))
p8 = qplot(sample = data_throughput_33600,  stat = "qq") + ggtitle(expression("33600 Mensagens"))

multiplot(p1, p2, p3, p4, p5, p6, p7, p8, cols=4)

ad.test(data_throughput_420)
ad.test(data_throughput_1260)
ad.test(data_throughput_2100)
ad.test(data_throughput_4200)
ad.test(data_throughput_8400)
ad.test(data_throughput_16800)
ad.test(data_throughput_25200)
ad.test(data_throughput_33600)



png("throughput.png")
p <- ggplot(data_final, aes(x = index , y=(ack_mean/event_mean), colour=type, group=type))+ 
  geom_line() +
  geom_errorbar(aes(ymin=ack_min/event_mean, ymax=ack_max/event_mean), width=.2) +
  geom_point() + 
  scale_x_discrete(breaks=data_final$index, labels=data_final$MessagesPerSecond) + scale_fill_hue("MessagesPerSecond") +
  xlab("Messages/s") +
  ylab("Acks/Messages")

print(p)

dev.off()
# ggplot(data_0, aes(x=MessagesPerSecond, y=Ack_Mean, colour="red")) + 
#   geom_errorbar(aes(ymin=Ack_Mean-(Ack_SD*1.96), ymax=Ack_Mean+(Ack_SD*1.96), width=1000) +
#   geom_point()