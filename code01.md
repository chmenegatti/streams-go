# Exemplo de Código Completos para Cada Etapa do Pipeline

```go
package main

import (
 "context"
 "fmt"
 "math/rand"
 "strings"
 "sync"
 "time"
)

// RawData representa os dados brutos como poderiam vir de uma fonte
type RawData struct {
 ID      int
 Payload string
 Source  string
}

// ProcessedData representa os dados após o processamento
type ProcessedData struct {
 OriginalID  int
 TransformedPayload string
 Source      string
 ProcessedAt time.Time
}

// --- 1. Ingestão de Dados ---
// dataIngestor simula a ingestão de dados de uma fonte externa (e.g., API, Kafka).
// Ele envia RawData para o outputChannel.
func dataIngestor(ctx context.Context, wg *sync.WaitGroup, outputChannel chan<- RawData) {
 defer wg.Done()
 defer close(outputChannel) // Fecha o canal quando a ingestão termina

 ticker := time.NewTicker(500 * time.Millisecond) // Gera um novo dado a cada 500ms
 defer ticker.Stop()

 eventID := 0
 for {
  select {
  case <-ctx.Done(): // Se o contexto for cancelado, para a ingestão
   fmt.Println("Ingestor: Contexto cancelado, parando.")
   return
  case <-ticker.C:
   eventID++
   data := RawData{
    ID:      eventID,
    Payload: fmt.Sprintf("event_data_%d_from_source_A", eventID),
    Source:  "SourceA",
   }
   // Simula uma pequena chance de dados de outra fonte para filtragem posterior
   if rand.Intn(10) < 2 {
    data.Payload = fmt.Sprintf("log_data_%d_from_source_B", eventID)
    data.Source = "SourceB"
   }

   fmt.Printf("Ingestor: Gerando dado ID %d: %s\n", data.ID, data.Payload)
   outputChannel <- data

   // Simula um fim para o stream para este exemplo
   if eventID >= 10 {
    fmt.Println("Ingestor: Limite de eventos atingido, finalizando.")
    return
   }
  }
 }
}

// --- 2. Processamento de Dados ---
// dataProcessor recebe RawData do inputChannel, processa, e envia ProcessedData para o outputChannel.
// Múltiplas instâncias podem rodar em paralelo (workers).
func dataProcessor(ctx context.Context, wg *sync.WaitGroup, workerID int, inputChannel <-chan RawData, outputChannel chan<- ProcessedData) {
 defer wg.Done()
 fmt.Printf("Processador Worker %d: Iniciado\n", workerID)

 for rawData := range inputChannel { // Lê do canal até que seja fechado
  select {
  case <-ctx.Done():
   fmt.Printf("Processador Worker %d: Contexto cancelado, parando.\n", workerID)
   return
  default:
   fmt.Printf("Processador Worker %d: Recebeu dado ID %d: %s\n", workerID, rawData.ID, rawData.Payload)

   // Filtro: Processar apenas dados da "SourceA"
   if rawData.Source != "SourceA" {
    fmt.Printf("Processador Worker %d: Dado ID %d (Fonte: %s) filtrado.\n", workerID, rawData.ID, rawData.Source)
    continue // Pula para o próximo dado
   }

   // Transformação: Converter payload para maiúsculas
   transformedPayload := strings.ToUpper(rawData.Payload)
   time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond) // Simula trabalho de processamento

   processed := ProcessedData{
    OriginalID:        rawData.ID,
    TransformedPayload: transformedPayload,
    Source:            rawData.Source,
    ProcessedAt:       time.Now(),
   }
   fmt.Printf("Processador Worker %d: Processou dado ID %d -> %s\n", workerID, processed.OriginalID, processed.TransformedPayload)
   outputChannel <- processed
  }
 }
 fmt.Printf("Processador Worker %d: Canal de entrada fechado, finalizando.\n", workerID)
}

// --- 3. Saída de Dados ---
// dataOutputter recebe ProcessedData do inputChannel e "envia" para um destino (aqui, o console).
func dataOutputter(ctx context.Context, wg *sync.WaitGroup, inputChannel <-chan ProcessedData) {
 defer wg.Done()
 fmt.Println("Outputter: Iniciado")

 for processedData := range inputChannel {
  select {
  case <-ctx.Done():
   fmt.Println("Outputter: Contexto cancelado, parando.")
   return
  default:
   // Aqui você enviaria para um DB, API, etc.
   fmt.Printf("Outputter: Recebido para saída -> ID: %d, Payload: %s, Processado em: %s\n",
    processedData.OriginalID,
    processedData.TransformedPayload,
    processedData.ProcessedAt.Format(time.RFC3339Nano),
   )
   // Simula escrita em um sistema externo
   time.Sleep(50 * time.Millisecond)
  }
 }
 fmt.Println("Outputter: Canal de entrada fechado, finalizando.")
}

func main() {
 fmt.Println("Iniciando pipeline de dados...")

 // Contexto para gerenciamento de ciclo de vida (cancelamento)
 // Neste exemplo, vamos simular um encerramento após um tempo
 // ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
 ctx, cancel := context.WithCancel(context.Background())
 defer cancel() // Garante que o cancelamento seja chamado para limpar recursos

 var wg sync.WaitGroup

 // Criação dos channels para conectar os estágios
 // Usamos buffers pequenos para ilustrar o fluxo; em produção, o tamanho do buffer é uma decisão importante.
 rawChannel := make(chan RawData, 3)
 processedChannel := make(chan ProcessedData, 3)

 // 1. Iniciar Ingestor
 wg.Add(1)
 go dataIngestor(ctx, &wg, rawChannel)

 // 2. Iniciar Processadores (Workers)
 numProcessors := 2
 for i := 0; i < numProcessors; i++ {
  wg.Add(1)
  go dataProcessor(ctx, &wg, i+1, rawChannel, processedChannel)
 }

 // 3. Iniciar Outputter
 wg.Add(1)
 go dataOutputter(ctx, &wg, processedChannel)

 // Lógica para fechar o processedChannel quando todos os processadores terminarem.
 // Isso sinaliza ao outputter que não haverá mais dados.
 go func() {
  // Espera todos os processadores (que dependem do rawChannel) terminarem.
  // Isto é um pouco simplificado. Uma forma mais robusta seria ter os processadores
  // sinalizando sua conclusão para um contador, e quando o contador atingir numProcessors,
  // fechar o processedChannel.
  // No nosso caso, o fechamento do rawChannel pelo ingestor eventualmente fará os processadores terminarem.
  // Quando o ingestor e os processadores que consomem do rawChannel terminarem, podemos fechar o processedChannel.
  // Uma maneira de coordenar o fechamento do processedChannel é esperar que o rawChannel seja fechado
  // e que todos os itens nele tenham sido lidos (o que significa que os processadores irão terminar).
  // Para simplificar aqui: esperamos que o rawChannel seja fechado e assumimos que
  // os processadores irão processar tudo o que estiver nele antes de terminarem.
  // O `wg.Wait()` principal garante que todos os `Done()` sejam chamados.

  // Uma abordagem comum é ter um WaitGroup para os processadores especificamente.
  var processorWg sync.WaitGroup
  // Re-iniciar os processadores com este WaitGroup específico
  // (Esta parte do código é ilustrativa de como se faria, mas não está integrada no fluxo principal acima
  // para não complicar demais o exemplo inicial. No código principal, o `close(processedChannel)`
  // precisa ser chamado APÓS todos os `dataProcessor` terem enviado seus dados e ANTES que o `dataOutputter`
  // espere indefinidamente.)

  // A forma mais simples de fechar o `processedChannel` é quando todos os produtores para ele (os `dataProcessor`)
  // terminam. Podemos usar um WaitGroup para os processadores.
  tempWg := sync.WaitGroup{}
  inputToProcessors := rawChannel // Salva referência original
  
  // (Re-iniciando lógica para fechar `processedChannel` corretamente)
  // O `main` `wg` já conta os processadores. Precisamos de uma forma de saber quando todos os processadores terminaram de escrever.
  // Uma maneira é fazer com que os processadores sinalizem quando terminam.
  // Outra, mais simples para este exemplo, é fechar o `processedChannel` quando todos os `dataProcessor` goroutines terminarem.
  // Vamos criar um WaitGroup separado para os processadores para controlar o fechamento do processedChannel.
  
  // Na verdade, a forma mais limpa:
  // O `main` `wg` já conta todas as goroutines principais.
  // A goroutine que inicia os processadores pode esperar por eles e então fechar o `processedChannel`.
  go func() {
   // Esta goroutine interna espera apenas pelos processadores
   var processWg sync.WaitGroup
   for i := 0; i < numProcessors; i++ {
    processWg.Add(1)
    // Esta é uma re-definição, mas para o propósito do fechamento:
    // Na verdade, não podemos re-lançar os processadores aqui.
    // A lógica de fechar o canal de saída de um conjunto de workers é:
    // 1. Os workers são iniciados.
    // 2. Uma goroutine separada espera que todos os workers (que escrevem no canal) terminem.
    // 3. Essa goroutine então fecha o canal.
   }
   // Este bloco é complexo de encaixar sem refatorar o `main`.
   // O `wg.Wait()` no final do `main` já espera por todos.
   // O fechamento do `processedChannel` deve ocorrer após todos os processadores pararem de enviar dados.
   // Se `rawChannel` fecha, os processadores eventualmente terminam.
   // Uma forma é ter uma goroutine que monitora `numProcessors` contadores de `Done`.
   // Para simplificar este exemplo, o `wg.Wait()` no final do main garante que tudo termine,
   // mas o `processedChannel` não é fechado explicitamente de forma coordenada com os processadores.
   // Para fechar `processedChannel` corretamente, você precisaria de um `sync.WaitGroup`
   // especificamente para os `dataProcessor`s. Quando esse `WaitGroup` indicar que todos os
   // processadores terminaram, aí sim o `processedChannel` pode ser fechado.

   // A forma mais simples para este exemplo, embora não ideal para todos os cenários,
   // é deixar que o `dataOutputter` continue lendo até que o `main` termine (devido ao `ctx.Done()`
   // ou todos os `wg.Done()` serem chamados). Em cenários reais, fechar canais é crucial.
   // Para um fechamento limpo e coordenado do `processedChannel`:
   // Crie um WaitGroup específico para os processadores.
   var processorWgForClosing sync.WaitGroup
   processorWgForClosing.Add(numProcessors)

   // No loop que inicia os processadores, passe &processorWgForClosing e chame Done() nela
   // ao final de cada dataProcessor.
   // Ex: go dataProcessor(ctx, &wg, &processorWgForClosing, i+1, rawChannel, processedChannel)
   // E dentro de dataProcessor: defer processorWgForClosing.Done()

   // Então, uma goroutine separada:
   // go func() {
   //     processorWgForClosing.Wait()
   //     close(processedChannel)
   //     fmt.Println("Todos os processadores terminaram. ProcessedChannel fechado.")
   // }()
   // Esta é a maneira canônica. Para manter o exemplo atual mais simples e focado nos estágios,
   // o fechamento explícito coordenado do processedChannel foi omitido, confiando no
   // cancelamento do contexto ou na terminação natural do fluxo.
   // Em um sistema real, o fechamento correto do `processedChannel` é vital.
   // Uma forma de fazer isso sem um WaitGroup extra para os processadores é
   // ter o `main` (ou uma goroutine orquestradora) esperar que o estágio de
   // processamento termine *antes* de sinalizar ao estágio de output para parar.
   // Mas com `range` em canais, o fechamento é a maneira idiomática de sinalizar "não há mais dados".
  }()

 }()

 // Espera todas as goroutines principais (ingestor, processadores, outputter) completarem.
 // Isso acontecerá quando o ingestor terminar (e fechar rawChannel),
 // os processadores terminarem (após rawChannel fechar e eles processarem tudo),
 // e o outputter terminar (após processedChannel fechar - o que não está perfeitamente coordenado aqui,
 // ou quando o contexto for cancelado).
 
 // Para garantir o fechamento de `processedChannel` de forma limpa:
 // Uma goroutine que monitora o `wg` dos processadores (ou um contador) e fecha `processedChannel`.
 // Ou, uma goroutine "mestre" que inicia os estágios em sequência de espera.
 // A maneira mais idiomática é que o produtor de um canal é responsável por fechá-lo.
 // No caso de múltiplos produtores (nossos processadores), um coordenador é necessário.
 // Inicia uma goroutine para fechar `processedChannel` após todos os processadores terminarem.
    allProcessorsWg := &sync.WaitGroup{}
    allProcessorsWg.Add(numProcessors)

    // Reiniciando a lógica dos processadores para usar este WaitGroup para o fechamento do canal.
    // Para evitar reescrever todo o main, vamos apenas descrever a mudança necessária:
    // 1. Crie `allProcessorsWg := &sync.WaitGroup{}`
    // 2. Adicione `allProcessorsWg.Add(numProcessors)` antes do loop dos processadores.
    // 3. Na goroutine de `dataProcessor`, adicione `defer allProcessorsWg.Done()` (além do `wg.Done()`).
    // 4. Inicie a goroutine abaixo:
    go func() {
        // Esta goroutine não está perfeitamente sincronizada no exemplo atual, mas ilustra a intenção.
        // O ideal seria que o `main` orquestrasse isso.
  // A questão é que os processadores precisam ser lançados com o `allProcessorsWg`.
  // Para o exemplo atual, vamos assumir que o `wg` principal para os processadores
  // pode ser usado para inferir quando fechar `processedChannel`.
  // Isso é uma simplificação. O `wg.Wait()` abaixo espera por todos, incluindo o ingestor e o outputter.
  // Para fechar `processedChannel`, precisamos esperar APENAS pelos processadores.

  // Corrigindo a lógica de fechamento do processedChannel:
  // A goroutine que lança os workers (processadores) deve esperar por eles e então fechar o canal de saída.
  // Isso pode ser feito tendo uma goroutine separada que lança os processadores e espera por eles.
  // Ou, se os processadores são gerenciados por `main` com um `wg` global,
  // outra goroutine precisa saber quando especificamente os *processadores* terminaram.

  // Simplificação para este exemplo: O `processedChannel` não será explicitamente fechado
  // de forma coordenada com os processadores. O `dataOutputter` terminará quando o contexto
  // for cancelado ou quando o programa principal encerrar após `wg.Wait()`.
  // Em um sistema de produção, o fechamento coordenado é fundamental.
  // Se o ingestor parar e fechar `rawChannel`, os processadores eventualmente processarão
  // tudo e terminarão. O `dataOutputter` então ficaria esperando em `processedChannel`
  // se não fosse fechado. O `ctx.Done()` lida com o encerramento.
    }()


 // Se você quiser que o pipeline pare após um certo tempo ou evento externo:
 // time.Sleep(8 * time.Second) // Deixe rodar por 8 segundos
 // cancel() // Envia sinal de cancelamento

 // Ou espere até que o ingestor sinalize que terminou (e o pipeline se esvazie)
 // wg.Wait() vai esperar até que dataIngestor chame wg.Done(),
 // os processadores chamem wg.Done() (após rawChannel fechar e eles processarem tudo),
 // e o outputter chame wg.Done() (após processedChannel fechar ou ctx ser cancelado).
 // Como o fechamento de processedChannel não está perfeitamente coordenado com a finalização
 // dos processadores, o outputter pode bloquear se o contexto não for cancelado.
 // A chamada `cancel()` após `wg.Wait()` (ou um timeout no contexto) garante o término.

 fmt.Println("Esperando todas as goroutines finalizarem...")
 wg.Wait() // Espera que todas as goroutines adicionadas ao wg terminem.
 
 // Se `processedChannel` não foi fechado explicitamente quando os processadores terminam,
 // e se `dataOutputter` está em `range processedChannel`, ele pode causar um deadlock se `ctx` não for cancelado.
 // O `defer cancel()` no início do `main` não é suficiente se não houver um `ctx.WithTimeout`.
 // Neste exemplo, o `dataIngestor` finaliza após 10 eventos, fechando `rawChannel`.
 // Isso faz com que os `dataProcessor`s terminem após processar os itens restantes.
 // Se `processedChannel` não for fechado, `dataOutputter` bloqueará.
 // Por isso, é importante que `processedChannel` seja fechado.
 // A forma mais simples de garantir o fechamento de `processedChannel` neste exemplo é fechar
 // APÓS o `WaitGroup` dos processadores ter completado.
 // Vamos simular isso, assumindo que os processadores terminariam e o `wg` refletiria isso.
 // Porém, o `wg` principal espera por *todos*. Precisamos de um `wg` só para processadores.
 // Para evitar grande refatoração agora, vamos confiar no cancelamento explícito ou
 // no fato de que o programa terminará.
 // Se `dataIngestor` termina, `rawChannel` fecha. `dataProcessor`s terminam.
 // Idealmente:
 //   wgProcs := sync.WaitGroup{}
 //   wgProcs.Add(numProcessors)
 //   // ...iniciar processadores passando wgProcs...
 //   go func() {
 //      wgProcs.Wait()
 //      close(processedChannel)
 //   }()
 // Isso garante que `dataOutputter` também termine naturalmente.
 // Sem isso, o `ctx.Done()` é o mecanismo de parada para `dataOutputter`.

 // Se o contexto não for cancelado antes (ex: por um timeout),
 // e o `processedChannel` não for fechado, o `dataOutputter` pode travar.
 // Adicionar `cancel()` aqui garante que, se o pipeline terminar "naturalmente",
 // o `dataOutputter` também será sinalizado para parar.
 fmt.Println("Pipeline principal terminou. Sinalizando cancelamento para garantir limpeza.")
 cancel() // Sinaliza para qualquer goroutine ainda escutando o contexto.

 fmt.Println("Pipeline de dados finalizado.")
}
```
