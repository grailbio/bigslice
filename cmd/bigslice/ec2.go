// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/config"
	"github.com/grailbio/base/must"

	// We bring these in so we can show the user all the defaults when
	// writing the profile.
	_ "github.com/grailbio/base/config/aws"
	_ "github.com/grailbio/bigmachine/ec2system"
	_ "github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceconfig"
)

func setupEc2Usage(flags *flag.FlagSet) {
	fmt.Fprint(os.Stderr, `usage: bigslice setup-ec2 [-securitygroup name]

Command setup-ec2 sets up a security group so that Bigslice programs
can run on AWS EC2. Once complete, the resulting configuration is
written to the Bigslice configuration file at `, sliceconfig.Path, `.
If a configuration file already exists, then it is modified in place.

Setup-ec2 tags the security group with the name "bigslice"; if a
previously set up security group already exists, no new group is
created, but the configuration is modified to include that security
group.

The Bigslice security group is set up with the following rules:

	allowed: all traffic within the default VPC
	allowed: all outbound
	allowed: inbound SSH connections
	allowed: inbound HTTPS connections

The flags are:
`)
	flags.PrintDefaults()
	os.Exit(2)
}

func setupEc2Cmd(args []string) {
	var (
		flags         = flag.NewFlagSet("bigslice setup-ec2", flag.ExitOnError)
		securityGroup = flags.String("securitygroup", "bigslice", "name of the security group to set up")
	)
	flags.Usage = func() { setupEc2Usage(flags) }
	flags.Parse(args)
	if flags.NArg() != 0 {
		flags.Usage()
	}

	profile := config.New()
	f, err := os.Open(sliceconfig.Path)
	if err == nil {
		must.Nil(profile.Parse(f))
		must.Nil(f.Close())
	} else {
		must.True(os.IsNotExist(err), err)
	}

	if region, ok := profile.Get("aws/env.region"); ok && len(region) > 0 {
		must.Nil(profile.Set("bigmachine/ec2system.default-region", strings.Trim(region, `"`)))
	}

	if v, ok := profile.Get("bigmachine/ec2system.security-group"); ok && v != `""` {
		log.Print("ec2 security group ", v, " already configured")
	} else {
		sess, err := session.NewSession()
		must.Nil(err, "setting up AWS session")
		ident, err := setupEC2SecurityGroup(ec2.New(sess), *securityGroup)
		must.Nil(err, "setting up security group")
		must.Nil(profile.Set("bigmachine/ec2system.security-group", ident))
		log.Print("set up new security group ", ident)
	}

	must.Nil(profile.Set("bigslice.system", "bigmachine/ec2system"))
	// Set up a more appropriate instance type for Bigslice.
	must.Nil(profile.Set("bigmachine/ec2system.instance", "m5.xlarge"))
	var buf bytes.Buffer
	must.Nil(profile.PrintTo(&buf))
	must.Nil(os.MkdirAll(filepath.Dir(sliceconfig.Path), 0777))
	must.Nil(ioutil.WriteFile(sliceconfig.Path+".setup-ec2", buf.Bytes(), 0777))
	must.Nil(os.Rename(sliceconfig.Path+".setup-ec2", sliceconfig.Path))
	log.Print("wrote configuration to ", sliceconfig.Path)
}

func setupEC2SecurityGroup(svc *ec2.EC2, name string) (string, error) {
	describeResp, err := svc.DescribeSecurityGroups(&ec2.DescribeSecurityGroupsInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("group-name"),
				Values: []*string{aws.String(name)},
			},
		},
	})
	if err != nil {
		if len(name) > 0 {
			return "", fmt.Errorf("unable to query existing security group: %v: %v", name, err)
		}
		return "", fmt.Errorf("no security group configured, and unable to query existing security groups: %v", err)
	}
	if len(describeResp.SecurityGroups) > 0 {
		id := aws.StringValue(describeResp.SecurityGroups[0].GroupId)
		log.Printf("found existing bigslice security group %s", id)
		return id, nil
	}
	log.Println("no existing bigslice security group found; creating new")
	// We are going to be launching into the default VPC, so find it.
	vpcResp, err := svc.DescribeVpcs(&ec2.DescribeVpcsInput{
		Filters: []*ec2.Filter{{
			Name:   aws.String("isDefault"),
			Values: []*string{aws.String("true")},
		}},
	})
	if err != nil {
		return "", fmt.Errorf("error retrieving default VPC while creating new security group:% v", err)
	}
	if len(vpcResp.Vpcs) == 0 {
		return "", errors.New(
			"AWS account does not have a default VPC and requires manual setup.\n" +
				"See https://docs.aws.amazon.com/vpc/latest/userguide/default-vpc.html#create-default-vpc")
	} else if len(vpcResp.Vpcs) > 1 {
		// I'm not sure this is possible. But keep it as a sanity check.
		return "", errors.New("AWS account has multiple default VPCs; needs manual setup")
	}
	vpc := vpcResp.Vpcs[0]
	log.Printf("found default VPC %s", aws.StringValue(vpc.VpcId))
	resp, err := svc.CreateSecurityGroup(&ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(name),
		Description: aws.String("security group automatically created by bigslice setup-ec2"),
		VpcId:       vpc.VpcId,
	})
	if err != nil {
		return "", fmt.Errorf("error creating security group %s: %v", name, err)
	}

	id := aws.StringValue(resp.GroupId)
	log.Printf("authorizing ingress traffic for security group %s", id)
	_, err = svc.AuthorizeSecurityGroupIngress(&ec2.AuthorizeSecurityGroupIngressInput{
		GroupName: aws.String(name),
		IpPermissions: []*ec2.IpPermission{
			// Allow all internal traffic.
			{
				IpProtocol: aws.String("-1"),
				IpRanges:   []*ec2.IpRange{{CidrIp: vpc.CidrBlock}},
				FromPort:   aws.Int64(0),
				ToPort:     aws.Int64(0),
			},
			// Allow incoming SSH connections.
			{
				IpProtocol: aws.String("tcp"),
				IpRanges:   []*ec2.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
				FromPort:   aws.Int64(22),
				ToPort:     aws.Int64(22),
			},
			// Allow incoming bigslice executor connections. (HTTPS)
			{
				IpProtocol: aws.String("tcp"),
				IpRanges:   []*ec2.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
				FromPort:   aws.Int64(443),
				ToPort:     aws.Int64(443),
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to authorize security group %s for ingress traffic: %v", id, err)
	}
	// The default egress rules are to permit all outgoing traffic.
	log.Printf("tagging security group %s", id)
	_, err = svc.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{aws.String(id)},
		Tags: []*ec2.Tag{
			{Key: aws.String("bigslice-sg"), Value: aws.String("true")},
			{Key: aws.String("Name"), Value: aws.String("bigslice")},
		},
	})
	if err != nil {
		log.Printf("tag security group %s: %v", id, err)
	}
	log.Printf("created security group %v", id)
	return id, nil
}
