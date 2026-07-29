use ersatztv_channel::config::ChannelConfig;
use ersatztv_channel::error::ChannelError;
use ersatztv_playout::playout::{PlayoutItem, PlayoutLoadResult, parse_playout_filename};
use time::OffsetDateTime;

pub struct PlayoutLoader {
    channel_config: ChannelConfig,
}

impl PlayoutLoader {
    pub fn new(channel_config: &ChannelConfig) -> PlayoutLoader {
        PlayoutLoader {
            channel_config: channel_config.to_owned(),
        }
    }

    pub async fn get_current_item(
        &self,
        now: &OffsetDateTime,
    ) -> Result<PlayoutItem, ChannelError> {
        // TODO: refactor selecting playout file

        log::debug!(
            "playout folder is {}",
            self.channel_config
                .expanded_playout_folder()
                .to_string_lossy()
        );

        let path = self.playout_file_for_time(now).await?;
        log::debug!("playout JSON is {path}");

        // load playout JSON
        let playout_result = ersatztv_playout::playout::from_file(&path).await?;

        // in case current item isn't found
        let next_start = self.next_start(&playout_result, now);

        // find current item
        playout_result
            .playout
            .items
            .into_iter()
            .rfind(|i| now >= &i.start && now < &i.finish())
            .ok_or(ChannelError::PlayoutJsonNoItem { next_start })
    }

    /// Finds a playout item by id in the playout file covering `now`. Used by
    /// variant sessions, which are told which item to transcode rather than
    /// following the schedule.
    pub async fn get_item_by_id(
        &self,
        item_id: &str,
        now: &OffsetDateTime,
    ) -> Result<PlayoutItem, ChannelError> {
        let path = self.playout_file_for_time(now).await?;
        let playout_result = ersatztv_playout::playout::from_file(&path).await?;

        playout_result
            .playout
            .items
            .into_iter()
            .find(|i| i.id == item_id)
            .ok_or(ChannelError::PlayoutJsonNoItem { next_start: None })
    }

    /// The `{query:}` variable names referenced anywhere in the playout file
    /// covering `now`, lowercased. Empty when no playout file covers `now`.
    pub async fn query_variable_names(
        &self,
        now: &OffsetDateTime,
    ) -> Result<std::collections::BTreeSet<String>, ChannelError> {
        let Ok(path) = self.playout_file_for_time(now).await else {
            return Ok(std::collections::BTreeSet::new());
        };

        let playout_result = ersatztv_playout::playout::from_file(&path).await?;

        let mut names = std::collections::BTreeSet::new();
        for item in &playout_result.playout.items {
            names.append(&mut item.query_variable_names());
        }

        Ok(names)
    }

    async fn playout_file_for_time(&self, now: &OffsetDateTime) -> Result<String, ChannelError> {
        let mut entries = tokio::fs::read_dir(self.channel_config.expanded_playout_folder())
            .await
            .map_err(|e| {
                ChannelError::ChannelConfigFailure(format!(
                    "{}: {:?}",
                    e,
                    self.channel_config.expanded_playout_folder()
                ))
            })?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path =
                entry.path().into_os_string().into_string().map_err(|_| {
                    ChannelError::ChannelConfigFailure(String::from("os string error"))
                })?;

            if let Some(file_name_os) = entry.path().file_stem() {
                let file_name = file_name_os.to_os_string().into_string().map_err(|_| {
                    ChannelError::ChannelConfigFailure(String::from("os string error"))
                })?;

                if let Some((start, finish)) = parse_playout_filename(file_name.as_str())
                    && now >= &start
                    && now < &finish
                {
                    return Ok(path);
                }
            }
        }

        Err(ChannelError::PlayoutJsonNoFileForTime(*now))
    }

    fn next_start(
        &self,
        playout_result: &PlayoutLoadResult,
        now: &OffsetDateTime,
    ) -> Option<OffsetDateTime> {
        playout_result
            .playout
            .items
            .iter()
            .find(|i| &i.start > now)
            .map(|i| i.start)
    }
}
