// Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// This software, the RabbitMQ Java client library, is triple-licensed under the
// Mozilla Public License 2.0 ("MPL"), the GNU General Public License version 2
// ("GPL") and the Apache License version 2 ("ASL"). For the MPL, please see
// LICENSE-MPL-RabbitMQ. For the GPL, please see LICENSE-GPL2.  For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

package com.rabbitmq.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Strategy interface to get the potential servers to connect to. */
public interface AddressResolver {

  /**
   * Get the potential {@link Address}es to connect to.
   *
   * @return candidate {@link Address}es
   * @throws IOException if it encounters a problem
   */
  List<Address> getAddresses() throws IOException;

  /**
   * Optionally shuffle the list of addresses returned by {@link #getAddresses()}.
   *
   * <p>The automatic connection recovery calls this method after {@link #getAddresses()} to pick a
   * random address for reconnecting.
   *
   * <p>The default method implementation calls {@link Collections#shuffle(List)}. Custom
   * implementations can choose to not do any shuffling to have more predictability in the
   * reconnection.
   *
   * @param input
   * @return potentially shuffled list of addresses.
   */
  default List<Address> maybeShuffle(List<Address> input) {
    List<Address> list = new ArrayList<Address>(input);
    Collections.shuffle(list);
    return list;
  }
}
