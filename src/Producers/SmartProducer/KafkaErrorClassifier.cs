using Confluent.Kafka;

namespace Kafka.Examples.Producers.SmartProducer;

public static class KafkaErrorClassifier
{
    public static readonly HashSet<ErrorCode> RetriableErrorCodes = [
        
        // network
        ErrorCode.BrokerNotAvailable, // брокер недоступен
        ErrorCode.ReplicaNotAvailable, // одна из реплик недоступна
        ErrorCode.NetworkException, // сетевой разрыв
        
        ErrorCode.RequestTimedOut, // брокер не успел обработать запрос
        
        // metadata (leader election, metadata outdated, partition reassignment)
        ErrorCode.LeaderNotAvailable, // идёт выбор лидера
        ErrorCode.NotLeaderForPartition, // отправили запрос не лидеру партиции
        ErrorCode.UnknownLeaderEpoch, // метадата устарела
        ErrorCode.UnknownTopicOrPart, // метадата устарела
        ErrorCode.OffsetNotAvailable, // информация по offset'ам временно недоступна
        ErrorCode.PreferredLeaderNotAvailable,
        ErrorCode.EligibleLeadersNotAvailable,
        ErrorCode.ReassignmentInProgress,
        ErrorCode.StaleBrokerEpoch,
        ErrorCode.StaleCtrlEpoch,
        ErrorCode.RebootstrapRequired,
        ErrorCode.KafkaStorageError, // ошибка диска при получении доступа к файлу лога кафки
        
        // replication
        ErrorCode.NotEnoughReplicas, // недостаточно in-sync реплик в данный момент
        ErrorCode.NotEnoughReplicasAfterAppend, // сообщение записано в недостаточное кол-во синк реплик
        
        // throttling
        ErrorCode.ThrottlingQuotaExceeded, // кластер перегружен → нужно подождать
        
        // local
        ErrorCode.Local_MsgTimedOut, // не смогли отправить сообщение в указанное время
        ErrorCode.Local_Transport, // сбой транспорта брокера
        ErrorCode.Local_AllBrokersDown, // все брокеры упали
        ErrorCode.Local_TimedOut, // таймаут на операцию в кафке
        ErrorCode.Local_IsrInsuff,
        ErrorCode.Local_Retry,
        ErrorCode.Local_WaitCache,
        ErrorCode.Local_Resolve,
        ErrorCode.Local_TimedOutQueue,
        
        ErrorCode.Local_QueueFull, // внутренняя очередь сообщений продюсера переполнена, и он не может принять новое сообщение
                                   // лучше делать retry with backoff (sleep 10-50ms -> retry)
        
        ErrorCode.Unknown,
    ];

    public static readonly HashSet<ErrorCode> NonRetriableErrorCodes = [
        ErrorCode.MsgSizeTooLarge, // размер сообщения больше допустимого
        ErrorCode.RecordListTooLarge, // размер батча, который продюсер отправил брокеру, превышает допустимый лимит
        
        //authorization errors
        ErrorCode.TopicAuthorizationFailed,
        ErrorCode.ClusterAuthorizationFailed,
        ErrorCode.GroupAuthorizationFailed,
        ErrorCode.TransactionalIdAuthorizationFailed,
        
        //invalid configuration
        ErrorCode.InvalidRequiredAcks, // продюсер указал недопустимое значение acks
        ErrorCode.InvalidConfig,
        ErrorCode.InvalidRequest,
        ErrorCode.UnsupportedVersion, //клиент отправил запрос Kafka версии, которую брокер не поддерживает
        ErrorCode.UnsupportedCompressionType,
        ErrorCode.InvalidRecord, // брокер не смог провалидировать сообщение
        ErrorCode.InvalidMsg,
        ErrorCode.PolicyViolation,
        
        //topic does not exist in cluster
        ErrorCode.TopicException, // такого топика не существует
        ErrorCode.Local_UnknownTopic, // такого топика не существует
        ErrorCode.UnknownTopicId,
        
        //idempotency produce errors (retry может сломать sequence)
        ErrorCode.OutOfOrderSequenceNumber,
        ErrorCode.DuplicateSequenceNumber,
        ErrorCode.InvalidProducerEpoch,
        ErrorCode.UnknownProducerId,
        ErrorCode.ProducerFenced,
    ];
    
    public static bool IsRetryable(Error error) => !error.IsFatal && !NonRetriableErrorCodes.Contains(error.Code);
    
    //для partition-level проблем временно не даем выбирать партицию для отправки в нее сообщений
    public static bool ShouldPartitionCooldown(Error error)
    {
        return error.Code switch
        {
            ErrorCode.LeaderNotAvailable => true,
            ErrorCode.NotLeaderForPartition => true,
            ErrorCode.PreferredLeaderNotAvailable => true,
            ErrorCode.EligibleLeadersNotAvailable => true,

            ErrorCode.BrokerNotAvailable => true,
            ErrorCode.ReplicaNotAvailable => true,

            ErrorCode.KafkaStorageError => true,

            ErrorCode.NotEnoughReplicas => true,
            ErrorCode.NotEnoughReplicasAfterAppend => true,

            _ => false
        };
    }
}