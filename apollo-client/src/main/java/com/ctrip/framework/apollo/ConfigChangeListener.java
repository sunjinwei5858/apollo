/*
 * Copyright 2021 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo;

import com.ctrip.framework.apollo.model.ConfigChangeEvent;

/**
 * 3:RepositoryChangeListener、ConfigChangeListener负责处理配置发布事件
 * @author Jason Song(song_s@ctrip.com)
 */
public interface ConfigChangeListener {
  /**
   * Invoked when there is any config change for the namespace.
   * @param changeEvent the event for this change
   */
  void onChange(ConfigChangeEvent changeEvent);
}
