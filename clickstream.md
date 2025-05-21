# `clickstream_pipeline/main.go`

```go
package main

import (
 "context"
 "encoding/json"
 "fmt"
 "log"
 "math/rand"
 "os"
 "os/signal"
 "strings"
 "sync"
 "syscall"
 "time"

 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
 kafkaBroker      = "localhost:9092" // Mude se necessário
 clickTopic       = "user_clicks_topic"
 processedTopic   = "processed_clicks_aggregates_topic" // Não usado ativamente para output, mas conceitual
 consumerGroupID  = "click_processor_group"
 windowDuration   = 10 * time.Second
 reportInterval   = 10 * time.Second // Com que frequência reportar as janelas
)

// ClickEvent representa um clique do usuário
type ClickEvent struct {
 UserID         string    `json:"user_id"`
 PageURL        string    `json:"page_url"`
 ElementClicked string    `json:"element_clicked"`
 Timestamp      time.Time `json:"timestamp"`
}

// EnrichedClickEvent inclui o segmento do usuário
type EnrichedClickEvent struct {
 ClickEvent
 UserSegment string `json:"user_segment"`
}

// UserProfileService simula um serviço de busca de perfil de usuário
type UserProfileService struct {
 profiles map[string]string // userID -> segment
 mu       sync.RWMutex
}

func NewUserProfileService() *UserProfileService {
 return &UserProfileService{
  profiles: map[string]string{
   "user1": "premium",
   "user2": "free",
   "user3": "premium",
   "user4": "guest",
   "user5": "free",
  },
 }
}

func (ups *UserProfileService) GetUserSegment(userID string) (string, error) {
 ups.mu.RLock()
 defer ups.mu.RUnlock()
 segment, ok := ups.profiles[userID]
 if !ok {
  // Simula um usuário desconhecido ou um segmento padrão
  return "unknown", nil 
 }
 return segment, nil
}

// WindowAggregator agrega eventos em janelas de tempo
type WindowAggregator struct {
 window      map[string]int // userSegment -> count
 windowStart time.Time
 mu          sync.Mutex
}

func NewWindowAggregator() *WindowAggregator {
 return &WindowAggregator{
  window:      make(map[string]int),
  windowStart: time.Now(),
 }
}

// AddEvent adiciona um evento ao agregador. Se o evento estiver fora da janela atual,
// a janela atual é "emitida" (impressa) e uma nova janela é iniciada.
func (wa *WindowAggregator) AddEvent(event EnrichedClickEvent) {
 wa.mu.Lock()
 defer wa.mu.Unlock()

 // Esta lógica de janela é baseada no tempo de processamento.
 // Para janelas baseadas em tempo de evento, seria mais complexo (lidar com eventos tardios, watermarks).
 // Aqui, simplificamos para uma janela de "tumbling" baseada no tempo de processamento/relatório.
 
 // Na verdade, para agregação baseada em janela de tempo de evento,
 // precisaríamos de uma estrutura mais sofisticada.
 // Este agregador simples contará todos os eventos que chegam
 // e será "resetado" e reportado periodicamente pelo ticker principal.
 wa.window[event.UserSegment]++
}

// ReportAndReset retorna a agregação atual e reinicia a janela.
func (wa *WindowAggregator) ReportAndReset() map[string]int {
 wa.mu.Lock()
 defer wa.mu.Unlock()

 currentAggregation := make(map[string]int)
 for k, v := range wa.window {
  currentAggregation[k] = v
 }
 
 fmt.Printf("[%s] Agregação da Janela (últimos ~%s):\n", time.Now().Format(time.RFC3339), windowDuration)
 for segment, count := range currentAggregation {
  fmt.Printf("  Segmento '%s': %d cliques\n", segment, count)
 }

 // Reset para a próxima janela
 wa.window = make(map[string]int)
 wa.windowStart = time.Now()
 
 return currentAggregation // Poderia ser enviado para outro tópico Kafka ou DB
}


// --- Produtor de Cliques (Simulado) ---
func runClickProducer(ctx context.Context, p *kafka.Producer) {
 fmt.Println("Produtor de Cliques Kafka iniciado...")
 users := []string{"user1", "user2", "user3", "user4", "user5", "user6"}
 pages := []string{"/home", "/products/1", "/products/2", "/cart", "/checkout"}
 elements := []string{"buy_button", "image_carousel", "link_to_details", "add_to_cart"}

 ticker := time.NewTicker(200 * time.Millisecond) // Gera cliques rapidamente
 defer ticker.Stop()

 for {
  select {
  case <-ctx.Done():
   fmt.Println("Produtor de Cliques: Contexto cancelado, encerrando.")
   p.Flush(5000)
   return
  case <-ticker.C:
   click := ClickEvent{
    UserID:         users[rand.Intn(len(users))],
    PageURL:        pages[rand.Intn(len(pages))],
    ElementClicked: elements[rand.Intn(len(elements))],
    Timestamp:      time.Now(),
   }
   clickJSON, _ := json.Marshal(click)

   err := p.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &clickTopic, Partition: kafka.PartitionAny},
    Value:          clickJSON,
   }, nil) // nil para delivery channel, confiando no loop de eventos do produtor

   if err != nil {
    fmt.Printf("Produtor de Cliques: Falha ao produzir: %v\n", err)
   } else {
    // fmt.Printf("Produtor de Cliques: Enviou %s\n", string(clickJSON))
   }
  }
 }
}

// --- Processador de Cliques ---
func runClickProcessor(ctx context.Context, wg *sync.WaitGroup, c *kafka.Consumer, profileService *UserProfileService, aggregator *WindowAggregator) {
 defer wg.Done()
 fmt.Println("Processador de Cliques Kafka iniciado...")

 err := c.SubscribeTopics([]string{clickTopic}, nil)
 if err != nil {
  log.Fatalf("Processador: Falha ao subscrever: %s", err)
 }

 run := true
 for run {
  select {
  case <-ctx.Done():
   fmt.Println("Processador de Cliques: Contexto cancelado, encerrando.")
   run = false
  default:
   ev := c.Poll(100) // Poll com timeout para permitir verificação do ctx.Done()
   if ev == nil {
    continue
   }

   switch e := ev.(type) {
   case *kafka.Message:
    // fmt.Printf("Processador: Mensagem recebida: %s\n", string(e.Value))
    var click ClickEvent
    err := json.Unmarshal(e.Value, &click)
    if err != nil {
     log.Printf("Processador: Falha ao deserializar JSON: %v. Mensagem: %s", err, string(e.Value))
     continue
    }

    // 1. Enriquecimento
    segment, err := profileService.GetUserSegment(click.UserID)
    if err != nil {
     log.Printf("Processador: Falha ao buscar perfil para %s: %v", click.UserID, err)
     segment = "error_profile" // Lidar com erro
    }
    enrichedClick := EnrichedClickEvent{
     ClickEvent:  click,
     UserSegment: segment,
    }
    // fmt.Printf("Processador: Evento enriquecido: %+v\n", enrichedClick)

    // 2. Adicionar ao Agregador
    aggregator.AddEvent(enrichedClick)

    // Em uma implementação com commit manual:
    // _, err = c.CommitMessage(e)
    // if err != nil { log.Printf("Processador: Falha ao commitar offset: %v", err)}

   case kafka.Error:
    fmt.Fprintf(os.Stderr, "%% Processador: Erro: %v: %v\n", e.Code(), e)
    if e.IsFatal() {
     run = false
    }
   default:
    // fmt.Printf("Processador: Evento ignorado: %v\n", e)
   }
  }
 }
 fmt.Println("Processador de Cliques Kafka finalizado.")
}

func main() {
 ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
 defer stop()

 // --- Configuração do Produtor Kafka ---
 producerCfg := &kafka.ConfigMap{"bootstrap.servers": kafkaBroker}
 // Opcional: para maior robustez, adicione configurações como:
 // "acks": "all", // Garante que a mensagem foi escrita em todas as réplicas ISR
    // "retries": 3, // Tenta reenviar mensagens que falharam
    // "retry.backoff.ms": 1000, // Espera 1s entre retries
 // "enable.idempotence": "true", // Garante que as mensagens não serão duplicadas em caso de retries (requer acks=all, retries > 0, max.in.flight.requests.per.connection <= 5)
 p, err := kafka.NewProducer(producerCfg)
 if err != nil {
  log.Fatalf("Falha ao criar produtor principal: %s", err)
 }
 defer p.Close()

 // Goroutine para lidar com eventos de entrega do produtor principal (logs, erros)
 go func() {
  for e := range p.Events() {
   switch ev := e.(type) {
   case *kafka.Message:
    if ev.TopicPartition.Error != nil {
     fmt.Printf("Produtor Principal: Falha na entrega: %v\n", ev.TopicPartition)
    } else {
     // Log muito verboso para este exemplo
     // fmt.Printf("Produtor Principal: Mensagem entregue a %v\n", ev.TopicPartition)
    }
   case kafka.Error:
    fmt.Printf("Produtor Principal: Erro: %v\n", ev)
   }
  }
 }()

 // --- Configuração do Consumidor Kafka ---
 consumerCfg := &kafka.ConfigMap{
  "bootstrap.servers": kafkaBroker,
  "group.id":          consumerGroupID,
  "auto.offset.reset": "latest", // Processa apenas novas mensagens
  // "enable.auto.commit": "false", // Para commit manual
 }
 c, err := kafka.NewConsumer(consumerCfg)
 if err != nil {
  log.Fatalf("Falha ao criar consumidor principal: %s", err)
 }
 defer c.Close() // Fechar consumidor

 profileService := NewUserProfileService()
 aggregator := NewWindowAggregator()

 var wg sync.WaitGroup

 // Iniciar Produtor de Cliques
 go runClickProducer(ctx, p)

 // Iniciar Processador de Cliques
 wg.Add(1)
 go runClickProcessor(ctx, &wg, c, profileService, aggregator)

 // Goroutine para reportar agregações periodicamente
 reportTicker := time.NewTicker(reportInterval)
 defer reportTicker.Stop()

 go func() {
  for {
   select {
   case <-ctx.Done():
    fmt.Println("Reportador de Agregação: Contexto cancelado, encerrando.")
    // Reporte final antes de sair
    aggregator.ReportAndReset()
    return
   case <-reportTicker.C:
    aggregator.ReportAndReset()
   }
  }
 }()

 // Espera por sinal de interrupção (Ctrl+C)
 <-ctx.Done()
 fmt.Println("Sinal de encerramento recebido. Finalizando...")

 // Espera o processador terminar
 wg.Wait()

 // O produtor e o reportador também são encerrados pelo contexto.
 // p.Flush pode ser chamado aqui se não for feito no runClickProducer, mas é melhor lá.
 fmt.Println("Pipeline de cliques finalizado.")
}
```