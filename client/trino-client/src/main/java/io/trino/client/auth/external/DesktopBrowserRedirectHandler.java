/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client.auth.external;

import java.net.URI;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.awt.Desktop.Action.BROWSE;
import static java.awt.Desktop.getDesktop;
import static java.awt.Desktop.isDesktopSupported;

public final class DesktopBrowserRedirectHandler
        implements RedirectHandler
{
    @Override
    public void redirectTo(URI uri)
            throws RedirectException
    {
        try {
            if (!isDesktopSupported() || !getDesktop().isSupported(BROWSE)) {
                throw new RedirectException("Desktop Browser is not available. Make sure your Java process is not in headless mode (-Djava.awt.headless=false)");
            }

            getDesktop().browse(uri);
        }
        catch (Throwable e) {
            throwIfInstanceOf(e, RedirectException.class);
            throw new RedirectException("Failed to redirect", e);
        }
    }
}
