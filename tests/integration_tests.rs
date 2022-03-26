mod common;
use common::proto::Consistency;

#[tokio::test(flavor = "multi_thread")]
async fn test_database_connection() {
    let replica = common::SPReplica::new(1, Vec::new());

    tokio::task::spawn(async {
        let response = replica
            .execute_query(String::from("SELECT 1"), Consistency::RelaxedReads)
            .await;

        assert_eq!(response.len(), 1);
        assert_eq!(response[0], "1");
    })
    .await
    .unwrap();

    replica.halt_replica();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consistency_relaxed() {
    let replicas = common::SPCluster::new(3);
    let replica_one = replicas.get_replica_with_id(1);
    let replica_two = replicas.get_replica_with_id(1);
    let replica_three = replicas.get_replica_with_id(1);

    let replica_one_handler = tokio::task::spawn(async {
        replica_one
            .execute_query(
                String::from("CREATE TABLE test_consistency (i integer PRIMARY KEY)"),
                Consistency::RelaxedReads,
            )
            .await;

        replica_one
            .execute_query(
                String::from("INSERT INTO test_consistency (50)"),
                Consistency::RelaxedReads,
            )
            .await;
    })
    .await
    .unwrap();

    let res_one = replica_one.execute_query(
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    );

    let res_two = replica_two.execute_query(
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    );

    let res_three = replica_three.execute_query(
        String::from("SELECT i FROM test_consistency"),
        Consistency::RelaxedReads,
    );

    let res_one = res_one.await;
    let res_two = res_two.await;
    let res_three = res_three.await;

    assert_eq!(res_one.len(), 1);
    assert_eq!(res_two.len(), 1);
    assert_eq!(res_three.len(), 1);
    assert_eq!(res_one[0], res_two[0]);
    assert_eq!(res_one[0], res_three[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consistency_strong() {
    let replicas = common::SPCluster::new(3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replicas_read_and_write() {
    let replicas = common::SPCluster::new(3);
    /* write to one replica and assert that all replicas return the same value */
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replicas_read_and_write_and_shutdown_one_replica() {
    let replicas = common::SPCluster::new(3);
    /* write to one replica, shutdown a follower, and assert that the remaining replicas return same value */
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replicas_read_and_write_and_shutdown_leader() {
    let replicas = common::SPCluster::new(3);
    /* write to one replica, showdown the leader, and assert that the remaining replicas return same value */
}
