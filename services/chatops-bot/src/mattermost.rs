//! The parts of the Mattermost REST API (v4) the bot uses. Every call takes
//! the token to act with: the admin's session for setup, or a bot's access
//! token for posting as that bot.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Mattermost request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Mattermost answered {status} to {what}: {body}")]
    Status { status: u16, what: String, body: String },
}

impl Error {
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            Self::Http(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Deserialize)]
pub struct User {
    pub id: String,
    pub username: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Team {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelInfo {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BotInfo {
    pub user_id: String,
    pub username: String,
    #[serde(default)]
    pub delete_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessToken {
    pub id: String,
    #[serde(default)]
    pub description: String,
    /// Only set when the token is created.
    #[serde(default)]
    pub token: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Post {
    #[serde(default)]
    pub id: String,
    pub channel_id: String,
    #[serde(default)]
    pub root_id: String,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub props: Value,
}

#[derive(Debug, Clone)]
pub struct Mattermost {
    base_url: String,
    http: reqwest::Client,
}

impl Mattermost {
    pub fn new(base_url: &str) -> Self {
        Self { base_url: base_url.trim_end_matches('/').to_string(), http: reqwest::Client::new() }
    }

    /// The WebSocket endpoint for events.
    pub fn websocket_url(&self) -> String {
        format!("{}/api/v4/websocket", self.base_url.replacen("http", "ws", 1))
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> Result<T> {
        let mut request = self.http.request(method.clone(), format!("{}/api/v4{path}", self.base_url));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Status { status: status.as_u16(), what: format!("{method} {path}"), body });
        }
        Ok(response.json().await?)
    }

    /// Like `call`, but a 404 is `None`.
    async fn find<T: DeserializeOwned>(&self, path: &str, token: &str) -> Result<Option<T>> {
        match self.call(reqwest::Method::GET, path, Some(token), None).await {
            Ok(value) => Ok(Some(value)),
            Err(err) if err.status() == Some(404) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn ping(&self) -> Result<()> {
        self.call::<Value>(reqwest::Method::GET, "/system/ping", None, None).await.map(drop)
    }

    /// Logs in and returns the session token.
    pub async fn login(&self, login_id: &str, password: &str) -> Result<String> {
        let response = self
            .http
            .post(format!("{}/api/v4/users/login", self.base_url))
            .json(&json!({"login_id": login_id, "password": password}))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Status { status: status.as_u16(), what: "login".into(), body });
        }
        response.headers().get("Token").and_then(|v| v.to_str().ok()).map(str::to_string).ok_or_else(|| Error::Status {
            status: status.as_u16(),
            what: "login".into(),
            body: "no Token header".into(),
        })
    }

    /// Creates a user. Without a token this is open sign-up, and the first
    /// user on a new server becomes its system admin.
    pub async fn create_user(&self, token: Option<&str>, username: &str, email: &str, password: &str) -> Result<User> {
        let body = json!({"username": username, "email": email, "password": password});
        self.call(reqwest::Method::POST, "/users", token, Some(&body)).await
    }

    pub async fn user(&self, token: &str, user_id: &str) -> Result<User> {
        self.call(reqwest::Method::GET, &format!("/users/{user_id}"), Some(token), None).await
    }

    pub async fn user_by_username(&self, token: &str, username: &str) -> Result<Option<User>> {
        self.find(&format!("/users/username/{username}"), token).await
    }

    pub async fn team_by_name(&self, token: &str, name: &str) -> Result<Option<Team>> {
        self.find(&format!("/teams/name/{name}"), token).await
    }

    pub async fn create_team(&self, token: &str, name: &str, display_name: &str) -> Result<Team> {
        let body = json!({"name": name, "display_name": display_name, "type": "O"});
        self.call(reqwest::Method::POST, "/teams", Some(token), Some(&body)).await
    }

    pub async fn add_team_member(&self, token: &str, team_id: &str, user_id: &str) -> Result<()> {
        let body = json!({"team_id": team_id, "user_id": user_id});
        self.call::<Value>(reqwest::Method::POST, &format!("/teams/{team_id}/members"), Some(token), Some(&body))
            .await
            .map(drop)
    }

    pub async fn channel_by_name(&self, token: &str, team_id: &str, name: &str) -> Result<Option<ChannelInfo>> {
        self.find(&format!("/teams/{team_id}/channels/name/{name}"), token).await
    }

    pub async fn create_channel(
        &self,
        token: &str,
        team_id: &str,
        name: &str,
        display_name: &str,
        purpose: &str,
    ) -> Result<ChannelInfo> {
        let body =
            json!({"team_id": team_id, "name": name, "display_name": display_name, "purpose": purpose, "type": "O"});
        self.call(reqwest::Method::POST, "/channels", Some(token), Some(&body)).await
    }

    pub async fn add_channel_member(&self, token: &str, channel_id: &str, user_id: &str) -> Result<()> {
        let body = json!({"user_id": user_id});
        self.call::<Value>(reqwest::Method::POST, &format!("/channels/{channel_id}/members"), Some(token), Some(&body))
            .await
            .map(drop)
    }

    pub async fn rename_channel(&self, token: &str, channel_id: &str, name: &str, display_name: &str) -> Result<()> {
        let body = json!({"name": name, "display_name": display_name});
        self.call::<Value>(reqwest::Method::PUT, &format!("/channels/{channel_id}/patch"), Some(token), Some(&body))
            .await
            .map(drop)
    }

    /// Archives a channel (Mattermost calls it deleting).
    pub async fn archive_channel(&self, token: &str, channel_id: &str) -> Result<()> {
        self.call::<Value>(reqwest::Method::DELETE, &format!("/channels/{channel_id}"), Some(token), None)
            .await
            .map(drop)
    }

    pub async fn bots(&self, token: &str) -> Result<Vec<BotInfo>> {
        self.call(reqwest::Method::GET, "/bots?include_deleted=true&per_page=200", Some(token), None).await
    }

    pub async fn create_bot(
        &self,
        token: &str,
        username: &str,
        display_name: &str,
        description: &str,
    ) -> Result<BotInfo> {
        let body = json!({"username": username, "display_name": display_name, "description": description});
        self.call(reqwest::Method::POST, "/bots", Some(token), Some(&body)).await
    }

    pub async fn enable_bot(&self, token: &str, user_id: &str) -> Result<()> {
        self.call::<Value>(reqwest::Method::POST, &format!("/bots/{user_id}/enable"), Some(token), None).await.map(drop)
    }

    pub async fn tokens(&self, token: &str, user_id: &str) -> Result<Vec<AccessToken>> {
        self.call(reqwest::Method::GET, &format!("/users/{user_id}/tokens?per_page=200"), Some(token), None).await
    }

    pub async fn revoke_token(&self, token: &str, token_id: &str) -> Result<()> {
        let body = json!({"token_id": token_id});
        self.call::<Value>(reqwest::Method::POST, "/users/tokens/revoke", Some(token), Some(&body)).await.map(drop)
    }

    pub async fn create_token(&self, token: &str, user_id: &str, description: &str) -> Result<AccessToken> {
        let body = json!({"description": description});
        self.call(reqwest::Method::POST, &format!("/users/{user_id}/tokens"), Some(token), Some(&body)).await
    }

    pub async fn create_post(&self, token: &str, post: &Post) -> Result<Post> {
        let body = json!({
            "channel_id": post.channel_id,
            "root_id": post.root_id,
            "message": post.message,
            "props": post.props,
        });
        self.call(reqwest::Method::POST, "/posts", Some(token), Some(&body)).await
    }

    /// Replaces a post's message and props.
    pub async fn patch_post(&self, token: &str, post_id: &str, message: &str, props: &Value) -> Result<Post> {
        let body = json!({"message": message, "props": props});
        self.call(reqwest::Method::PUT, &format!("/posts/{post_id}/patch"), Some(token), Some(&body)).await
    }

    pub async fn get_post(&self, token: &str, post_id: &str) -> Result<Post> {
        self.call(reqwest::Method::GET, &format!("/posts/{post_id}"), Some(token), None).await
    }
}
