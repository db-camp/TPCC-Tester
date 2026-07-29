#[path = "../src/profile.rs"]
mod profile;
#[path = "../src/routing.rs"]
mod routing;

use profile::{Final2026Profile, MEASUREMENT_WINDOWS};
use routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};

#[test]
fn public_profile_and_router_form_a_reproducible_stage_contract() {
    let profile = Final2026Profile::official();
    assert!(profile.is_ranked_configuration());

    let router = OfficialRouter::new(WorkloadSeed(2026));
    for window in 0..MEASUREMENT_WINDOWS {
        let wheel = router.wheel(StageId::measurement(window));
        for client_id in 0..profile.clients {
            let mut sequence = ClientSequence::new(client_id).unwrap();
            let first = router.begin_transaction(&wheel, &mut sequence).unwrap();
            assert_eq!(first.txn_no, 0);
            assert_eq!(sequence.next_txn_no(), 1);
        }
    }
}
