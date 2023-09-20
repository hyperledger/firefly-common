// Copyright © 2021 Kaleido, Inc.
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

package ffapi

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildFilterDescending(t *testing.T) {
	as := &HandlerFactory{
		MaxFilterLimit: 250,
	}

	req := httptest.NewRequest("GET", "/things?created=0&updated=!0&Tag=>abc&TAG=<abc&tag=<=abc&tag=>=abc&tag=@abc&tag=^abc&tag=!@abc&tag=!^abc&skip=10&limit=50&sort=tag,sequence&descending", nil)
	filter, err := as.buildFilter(req, TestQueryFactory)
	assert.NoError(t, err)
	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( created == 0 ) && ( ( tag !% 'abc' ) || ( tag !^ 'abc' ) || ( tag <= 'abc' ) || ( tag << 'abc' ) || ( tag >= 'abc' ) || ( tag >> 'abc' ) || ( tag %= 'abc' ) || ( tag ^= 'abc' ) ) && ( updated != 0 ) sort=-tag,-sequence skip=10 limit=50", fi.String())
}

func testIndividualFilter(t *testing.T, queryString, expectedToString string) {
	as := &HandlerFactory{
		MaxFilterLimit: 250,
	}
	req := httptest.NewRequest("GET", fmt.Sprintf("/things?%s", queryString), nil)
	filter, err := as.buildFilter(req, TestQueryFactory)
	assert.NoError(t, err)
	fi, err := filter.Finalize()
	assert.NoError(t, err)
	assert.Equal(t, expectedToString, fi.String())
}

func TestBuildFilterEachCombo(t *testing.T) {
	testIndividualFilter(t, "tag=cat", "( tag == 'cat' )")
	testIndividualFilter(t, "tag==cat", "( tag == 'cat' )")
	testIndividualFilter(t, "tag===cat", "( tag == '=cat' )")
	testIndividualFilter(t, "tag=!cat", "( tag != 'cat' )")
	testIndividualFilter(t, "tag=!=cat", "( tag != 'cat' )")
	testIndividualFilter(t, "tag=!=!cat", "( tag != '!cat' )")
	testIndividualFilter(t, "tag=!==cat", "( tag != '=cat' )")
	testIndividualFilter(t, "tag=!:=cat", "( tag ;= 'cat' )")
	testIndividualFilter(t, "tag=:!=cat", "( tag ;= 'cat' )")
	testIndividualFilter(t, "tag=:=cat", "( tag := 'cat' )")
	testIndividualFilter(t, "tag=>cat", "( tag >> 'cat' )")
	testIndividualFilter(t, "tag=>>cat", "( tag >> 'cat' )")
	testIndividualFilter(t, "tag=>>>cat", "( tag >> '>cat' )")
	testIndividualFilter(t, "tag=<cat", "( tag << 'cat' )")
	testIndividualFilter(t, "tag=<<cat", "( tag << 'cat' )")
	testIndividualFilter(t, "tag=<<<cat", "( tag << '<cat' )")
	testIndividualFilter(t, "tag=<>cat", "( tag << '>cat' )")
	testIndividualFilter(t, "tag=><cat", "( tag >> '<cat' )")
	testIndividualFilter(t, "tag=>=cat", "( tag >= 'cat' )")
	testIndividualFilter(t, "tag=<=cat", "( tag <= 'cat' )")
	testIndividualFilter(t, "tag=>=>cat", "( tag >= '>cat' )")
	testIndividualFilter(t, "tag=>==cat", "( tag >= '=cat' )")
	testIndividualFilter(t, "tag=@@cat", "( tag %= '@cat' )")
	testIndividualFilter(t, "tag=@cat", "( tag %= 'cat' )")
	testIndividualFilter(t, "tag=!@cat", "( tag !% 'cat' )")
	testIndividualFilter(t, "tag=:@cat", "( tag :% 'cat' )")
	testIndividualFilter(t, "tag=!:@cat", "( tag ;% 'cat' )")
	testIndividualFilter(t, "tag=^cat", "( tag ^= 'cat' )")
	testIndividualFilter(t, "tag=!^cat", "( tag !^ 'cat' )")
	testIndividualFilter(t, "tag=:^cat", "( tag :^ 'cat' )")
	testIndividualFilter(t, "tag=!:^cat", "( tag ;^ 'cat' )")
	testIndividualFilter(t, "tag=$cat", "( tag $= 'cat' )")
	testIndividualFilter(t, "tag=!$cat", "( tag !$ 'cat' )")
	testIndividualFilter(t, "tag=:$cat", "( tag :$ 'cat' )")
	testIndividualFilter(t, "tag=!:$cat", "( tag ;$ 'cat' )")
	testIndividualFilter(t, "tag==", "( tag == '' )")
	testIndividualFilter(t, "tag=!=", "( tag != '' )")
	testIndividualFilter(t, "tag=:!=", "( tag ;= '' )")
	testIndividualFilter(t, "tag=?", "( tag == null )")
	testIndividualFilter(t, "tag=!?", "( tag != null )")
	testIndividualFilter(t, "tag=?=", "( tag == null )")
	testIndividualFilter(t, "tag=!?=", "( tag != null )")
	testIndividualFilter(t, "tag=?:!=", "( tag ;= null )")
	testIndividualFilter(t, "tag=bob&tag=bob", "( ( tag == 'bob' ) || ( tag == 'bob' ) )")
	testIndividualFilter(t, "tag=[bob&tag=bob", "( ( tag == 'bob' ) && ( tag == 'bob' ) )")
	testIndividualFilter(t, "tag=[]bob&tag=bob", "( ( tag == 'bob' ) || ( tag == 'bob' ) )")
}

func testFailFilter(t *testing.T, queryString, errCode string) {
	as := &HandlerFactory{
		MaxFilterLimit: 250,
	}
	req := httptest.NewRequest("GET", fmt.Sprintf("/things?%s", queryString), nil)
	_, err := as.buildFilter(req, TestQueryFactory)
	assert.Regexp(t, errCode, err)
}

func TestCheckNoMods(t *testing.T) {
	testFailFilter(t, "tag=!>=test", "FF00193")
	testFailFilter(t, "tag=:>test", "FF00193")
	testFailFilter(t, "tag=!<test", "FF00193")
	testFailFilter(t, "tag=!:<=test", "FF00193")
	testFailFilter(t, "tag=<=test&tag=!<=test", "FF00193")
}

func TestBuildFilterAscending(t *testing.T) {
	as := &HandlerFactory{
		MaxFilterLimit: 250,
	}

	req := httptest.NewRequest("GET", "/things?created=0&sort=tag,sequence&ascending", nil)
	filter, err := as.buildFilter(req, TestQueryFactory)
	assert.NoError(t, err)
	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( created == 0 ) sort=tag,sequence", fi.String())
}

func TestBuildFilterLimitSkip(t *testing.T) {
	as := &HandlerFactory{
		MaxFilterSkip: 250,
	}

	req := httptest.NewRequest("GET", "/things?skip=251", nil)
	_, err := as.buildFilter(req, TestQueryFactory)
	assert.Regexp(t, "FF00191.*250", err)
}

func TestBuildFilterLimitLimit(t *testing.T) {
	as := &HandlerFactory{
		MaxFilterLimit: 500,
	}

	req := httptest.NewRequest("GET", "/things?limit=501", nil)
	_, err := as.buildFilter(req, TestQueryFactory)
	assert.Regexp(t, "FF00192.*500", err)
}

func TestBuildFilterRequiredFields(t *testing.T) {
	as := &HandlerFactory{
		MaxFilterLimit:        250,
		SupportFieldRedaction: true,
	}

	req := httptest.NewRequest("GET", "/things?created=0&fields=tag,sequence", nil)
	filter, err := as.buildFilter(req, TestQueryFactory)
	assert.NoError(t, err)
	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( created == 0 ) requiredFields=tag,sequence", fi.String())
}

func TestBuildFilterExtraFields(t *testing.T) {
	as := &HandlerFactory{
		MaxFilterLimit: 250,
	}

	req := httptest.NewRequest("GET", "/things?created=0&extraFields=tag,sequence", nil)
	filter, err := as.buildFilter(req, TestQueryFactory)
	assert.NoError(t, err)
	fi, err := filter.Finalize()
	assert.NoError(t, err)

	assert.Equal(t, "( created == 0 ) extraFields=tag,sequence", fi.String())
}
