// Copyright © 2023 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"

	"github.com/spf13/cobra"
)

func ShowConfigCommand(initConf func() error) *cobra.Command {
	return &cobra.Command{
		Use:     "showconfig",
		Aliases: []string{"showconf"},
		Short:   "List out the configuration options",
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := initConf(); err != nil {
				return err
			}

			fmt.Printf("%-64s %v\n", "Key", "Value")
			fmt.Print("-----------------------------------------------------------------------------------\n")
			for _, k := range GetKnownKeys() {
				fmt.Printf("%-64s %v\n", k, Get(RootKey(k)))
			}
			return nil
		},
	}
}
