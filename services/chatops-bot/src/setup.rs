//! Sets up the Mattermost workspace on every start, idempotently: the admin
//! (the on-call engineer people log in as), the `platform` team, the
//! channels, and the bot accounts with fresh access tokens.

use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use platform_clients::chatops::{Bot, Channel};
use tracing::{info, warn};

use crate::mattermost::Mattermost;

/// Access tokens the bot creates carry this description, so a restart can
/// revoke its old ones.
const TOKEN_DESCRIPTION: &str = "chatops-bot";

#[derive(Debug, Clone)]
pub struct Admin {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct BotAccount {
    pub user_id: String,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct Workspace {
    pub admin_token: String,
    pub admin_id: String,
    pub team_id: String,
    pub channels: HashMap<Channel, String>,
    pub bots: HashMap<Bot, BotAccount>,
}

impl Workspace {
    pub fn channel_id(&self, channel: Channel) -> &str {
        &self.channels[&channel]
    }

    pub fn bot_token(&self, bot: Bot) -> &str {
        &self.bots[&bot].token
    }

    pub fn is_bot(&self, user_id: &str) -> bool {
        self.bots.values().any(|b| b.user_id == user_id)
    }

    fn members(&self) -> Vec<String> {
        std::iter::once(self.admin_id.clone()).chain(self.bots.values().map(|b| b.user_id.clone())).collect()
    }
}

pub async fn bootstrap(mm: &Mattermost, admin: &Admin, team_name: &str) -> anyhow::Result<Workspace> {
    for attempt in 1.. {
        match mm.ping().await {
            Ok(()) => break,
            Err(err) if attempt < 90 => {
                if attempt % 10 == 1 {
                    warn!(%err, "Mattermost not ready; waiting");
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => return Err(err).context("waiting for Mattermost"),
        }
    }

    let admin_token = match mm.login(&admin.username, &admin.password).await {
        Ok(token) => token,
        Err(err) if err.status() == Some(401) => {
            info!(username = admin.username, "creating the admin user");
            let email = format!("{}@platform.example", admin.username);
            mm.create_user(None, &admin.username, &email, &admin.password).await.context("creating the admin user")?;
            mm.login(&admin.username, &admin.password).await.context("logging in as the admin")?
        }
        Err(err) => return Err(err).context("logging in as the admin"),
    };
    let admin_id = mm.user_by_username(&admin_token, &admin.username).await?.context("the admin user vanished")?.id;

    let team_id = match mm.team_by_name(&admin_token, team_name).await? {
        Some(team) => team.id,
        None => mm.create_team(&admin_token, team_name, "Streaming Platform").await.context("creating the team")?.id,
    };
    mm.add_team_member(&admin_token, &team_id, &admin_id).await?;

    let existing = mm.bots(&admin_token).await.context("listing bots")?;
    let mut bots = HashMap::new();
    for bot in Bot::ALL {
        let user_id = match existing.iter().find(|b| b.username == bot.username()) {
            Some(found) => {
                if found.delete_at != 0 {
                    mm.enable_bot(&admin_token, &found.user_id).await?;
                }
                found.user_id.clone()
            }
            None => {
                mm.create_bot(&admin_token, bot.username(), bot.display_name(), bot.description())
                    .await
                    .with_context(|| format!("creating bot {}", bot.username()))?
                    .user_id
            }
        };
        mm.add_team_member(&admin_token, &team_id, &user_id).await?;
        for old in mm.tokens(&admin_token, &user_id).await? {
            if old.description == TOKEN_DESCRIPTION {
                mm.revoke_token(&admin_token, &old.id).await?;
            }
        }
        let token = mm.create_token(&admin_token, &user_id, TOKEN_DESCRIPTION).await?.token;
        bots.insert(bot, BotAccount { user_id, token });
    }

    let mut workspace = Workspace { admin_token, admin_id, team_id, channels: HashMap::new(), bots };
    for channel in Channel::ALL {
        let id = ensure_channel(mm, &workspace, channel).await?;
        workspace.channels.insert(channel, id);
    }
    info!(team = team_name, channels = Channel::ALL.len(), bots = Bot::ALL.len(), "Mattermost workspace ready");
    Ok(workspace)
}

/// Finds or creates a channel, with the admin and every bot as members.
async fn ensure_channel(mm: &Mattermost, ws: &Workspace, channel: Channel) -> anyhow::Result<String> {
    let id = match mm.channel_by_name(&ws.admin_token, &ws.team_id, channel.name()).await? {
        Some(found) => found.id,
        None => {
            mm.create_channel(&ws.admin_token, &ws.team_id, channel.name(), channel.name(), channel.purpose())
                .await
                .with_context(|| format!("creating channel {}", channel.name()))?
                .id
        }
    };
    for member in ws.members() {
        mm.add_channel_member(&ws.admin_token, &id, &member).await?;
    }
    Ok(id)
}

/// Archives every platform channel and creates it afresh, for a clean demo
/// run (the director's reset). The archived ones are renamed with a
/// timestamp suffix, so they stay readable.
pub async fn reset_channels(mm: &Mattermost, ws: &mut Workspace) -> anyhow::Result<()> {
    let suffix = chrono::Utc::now().format("%Y%m%d-%H%M%S");
    for channel in Channel::ALL {
        if let Some(old) = mm.channel_by_name(&ws.admin_token, &ws.team_id, channel.name()).await? {
            let name = format!("{}-{suffix}", channel.name());
            mm.rename_channel(&ws.admin_token, &old.id, &name, &name).await?;
            mm.archive_channel(&ws.admin_token, &old.id).await?;
        }
        let id = ensure_channel(mm, ws, channel).await?;
        ws.channels.insert(channel, id);
    }
    info!(%suffix, "channels archived and re-created");
    Ok(())
}
