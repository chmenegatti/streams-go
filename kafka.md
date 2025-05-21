# `kafka_example/main.go`

```go
package main

import (
 "context"
 "fmt"
 "os"
 "os/signal"
 "syscall"
 "time"

 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
 kafkaBroker = "localhost:9092" // Mude se seu broker Kafka estiver em outro lugar
 topic       = "go_streaming_topic"
 groupID     = "go_streaming_group"
)

func runProducer(ctx context.Context) {
 p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
 if err != nil {
  panic(fmt.Sprintf("Falha ao criar produtor: %s", err))
 }
 defer p.Close()

 fmt.Println("Produtor Kafka iniciado...")

 // Goroutine para lidar com eventos de entrega (opcional, mas bom para produção)
 go func() {
  for e := range p.Events() {
   switch ev := e.(type) {
   case *kafka.Message:
    if ev.TopicPartition.Error != nil {
     fmt.Printf("Falha na entrega: %v\n", ev.TopicPartition)
    } else {
     fmt.Printf("Mensagem entregue ao tópico %s [%d] no offset %v\n",
      *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
    }
   case kafka.Error:
    fmt.Printf("Erro do produtor: %v\n", ev)
   }
  }
 }()

 messageCount := 0
 ticker := time.NewTicker(1 * time.Second)
 defer ticker.Stop()

 for {
  select {
  case <-ctx.Done():
   fmt.Println("Produtor: Contexto cancelado, encerrando.")
   // Espera por mensagens pendentes serem entregues/falharem antes de fechar
   // O Flush pode levar até o timeout configurado (padrão: infinito)
   // Para um encerramento mais rápido, use um timeout menor.
   remaining := p.Flush(5000) // Timeout de 5 segundos
   fmt.Printf("Produtor: %d mensagens restantes no buffer após flush.\n", remaining)
   return
  case <-ticker.C:
   messageCount++
   value := fmt.Sprintf("Mensagem de Go %d - %s", messageCount, time.Now().Format(time.RFC3339))
   
   deliveryChan := make(chan kafka.Event) // Canal para confirmação de entrega desta mensagem

   err = p.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          []byte(value),
    Key:            []byte(fmt.Sprintf("key-%d", messageCount%3)), // Chave para particionamento
   }, deliveryChan) // Usando deliveryChan para confirmação específica

   if err != nil {
    fmt.Printf("Falha ao enfileirar mensagem para produção: %v\n", err)
   } else {
    // Espera pela confirmação de entrega (opcional, mas bom para garantir)
    e := <-deliveryChan
    m := e.(*kafka.Message)
    if m.TopicPartition.Error != nil {
     fmt.Printf("Falha na entrega da mensagem: %v\n", m.TopicPartition.Error)
    } else {
     // fmt.Printf("Produtor: Mensagem '%s' enviada para %s [%d] @ %v\n",
     //  string(m.Value), *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
    }
   }
   close(deliveryChan)
  }
 }
}

func runConsumer(ctx context.Context, wg *sync.WaitGroup) {
 defer wg.Done()
 c, err := kafka.NewConsumer(&kafka.ConfigMap{
  "bootstrap.servers": kafkaBroker,
  "group.id":          groupID,
  "auto.offset.reset": "earliest", // Começa do início se não houver offset salvo
  // "enable.auto.commit": false, // Desabilitar auto commit para controle manual
 })

 if err != nil {
  panic(fmt.Sprintf("Falha ao criar consumidor: %s", err))
 }
 defer c.Close()

 err = c.SubscribeTopics([]string{topic}, nil)
 if err != nil {
  panic(fmt.Sprintf("Falha ao subscrever ao tópico: %s", err))
 }

 fmt.Println("Consumidor Kafka iniciado, esperando mensagens...")

 run := true
 for run {
  select {
  case <-ctx.Done():
   fmt.Println("Consumidor: Contexto cancelado, encerrando.")
   run = false
  default:
   // Poll por mensagens. Timeout de 100ms.
   // Em um loop de produção, você pode usar um timeout maior ou -1 para bloquear indefinidamente.
   // Para permitir o cancelamento via contexto, um timeout finito é melhor.
   ev := c.Poll(100)
   if ev == nil {
    continue // Timeout, nenhuma mensagem ou evento
   }

   switch e := ev.(type) {
   case *kafka.Message:
    fmt.Printf("Consumidor: Mensagem recebida do tópico %s [%d] offset %v: Key = %s, Value = %s\n",
     *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset,
     string(e.Key), string(e.Value))
    // Aqui você processaria a mensagem.
    // Se enable.auto.commit=false, você comitaria o offset aqui:
    // _, err := c.CommitMessage(e)
    // if err != nil {
    //    fmt.Printf("Falha ao commitar offset: %v\n", err)
    // }
   case kafka.Error:
    // Erros são geralmente informativos e não fatais.
    fmt.Fprintf(os.Stderr, "%% Erro do Consumidor: %v: %v\n", e.Code(), e)
    if e.IsFatal() {
     fmt.Fprintf(os.Stderr, "%% Erro fatal do consumidor, encerrando: %v\n", e)
     run = false // Encerra em erro fatal
    }
   case kafka.PartitionEOF:
    // Chegou ao fim da partição (não é um erro)
    // fmt.Printf("%% Atingido EOF para %v\n", e)
   default:
    // fmt.Printf("Evento ignorado: %v\n", e)
   }
  }
 }
 fmt.Println("Consumidor Kafka finalizado.")
}

func main() {
 // Cria um contexto que é cancelado com SIGINT ou SIGTERM
 ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
 defer stop()

 var wg sync.WaitGroup

 // Iniciar Produtor em uma goroutine
 go runProducer(ctx)

 // Iniciar Consumidor em uma goroutine
 wg.Add(1)
 go runConsumer(ctx, &wg)

 // Espera o contexto ser cancelado (e.g., Ctrl+C)
 <-ctx.Done()
 fmt.Println("Sinal de encerramento recebido. Aguardando consumidor finalizar...")
 
 // Espera o consumidor terminar graciosamente
 wg.Wait()
 fmt.Println("Aplicação Kafka encerrada.")
}
```