use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey};

struct DatabaseKey (String);

impl From<ActionInfoHashKey> for DatabaseKey {
    fn from(v: ActionInfoHashKey) -> Self {
        Self(hex::encode(v.get_hash()))
    }
}

impl From<&ActionInfoHashKey> for DatabaseKey {
    fn from(v: &ActionInfoHashKey) -> Self {
        Self(hex::encode(v.get_hash()))
    }
}

impl From<ActionInfo> for DatabaseKey {
    fn from(v: ActionInfo) -> Self { Self::from(v.unique_qualifier) }
}
impl From<&ActionInfo> for DatabaseKey {
    fn from(v: &ActionInfo) -> Self { Self::from(&v.unique_qualifier) }
}

trait DatabaseAdapter {

}
