// created by leipeng at 2019-10-17
// clang-format off
#pragma once
#include "rocksdb_namespace.h"

#define ROCKSDB_PP_EMPTY
#define ROCKSDB_PP_APPLY(func, ...) func(__VA_ARGS__)

///@param arg is parented such as (1,2,3)
///@returns parents are removed: (1,2,3) to 1,2,3
///@note ROCKSDB_PP_REMOVE_PARENT((1,2,3)) = 1,2,3
#define ROCKSDB_PP_REMOVE_PARENT(arg) ROCKSDB_PP_REMOVE_PARENT_AUX arg
#define ROCKSDB_PP_REMOVE_PARENT_AUX(...) __VA_ARGS__

#define ROCKSDB_PP_CAT2_1(a,b)    a##b
#define ROCKSDB_PP_CAT2(a,b)      ROCKSDB_PP_CAT2_1(a,b)
#define ROCKSDB_PP_CAT3(a,b,c)    ROCKSDB_PP_CAT2(ROCKSDB_PP_CAT2(a,b),c)
#define ROCKSDB_PP_CAT4(a,b,c,d)  ROCKSDB_PP_CAT2(ROCKSDB_PP_CAT3(a,b,c),d)

#define ROCKSDB_PP_EXTENT(arr) (sizeof(arr)/sizeof(arr[0]))

#define ROCKSDB_PP_IDENTITY_1(...) __VA_ARGS__
#define ROCKSDB_PP_IDENTITY_2(...) ROCKSDB_PP_IDENTITY_1(__VA_ARGS__)
#define ROCKSDB_PP_IDENTITY(x,...) ROCKSDB_PP_IDENTITY_2(x,##__VA_ARGS__)

#define ROCKSDB_PP_ARG_X(_0,_1,_2,_3,_4,_5,_6,_7,_8,_9, \
           a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z, \
           A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,XX,...) XX
#define ROCKSDB_PP_ARG_N(...) \
        ROCKSDB_PP_ARG_X("ignored", ##__VA_ARGS__, \
            Z,Y,X,W,V,U,T,S,R,Q,P,O,N,M,L,K,J,I,H,G,F,E,D,C,B,A, \
            z,y,x,w,v,u,t,s,r,q,p,o,n,m,l,k,j,i,h,g,f,e,d,c,b,a, \
                                            9,8,7,6,5,4,3,2,1,0)

#define ROCKSDB_PP_VA_NAME(prefix,...) \
        ROCKSDB_PP_CAT2(prefix,ROCKSDB_PP_ARG_N(__VA_ARGS__))

///@{
//#define ROCKSDB_PP_CAT_0()       error "ROCKSDB_PP_CAT" have at least 2 params
// allowing ROCKSDB_PP_CAT take just 1 argument
#define ROCKSDB_PP_CAT_0()
#define ROCKSDB_PP_CAT_1_1(x)     x
#define ROCKSDB_PP_CAT_1(x)       ROCKSDB_PP_CAT_1_1(x)
#define ROCKSDB_PP_CAT_2(x,y)     ROCKSDB_PP_CAT2(x,y)
#define ROCKSDB_PP_CAT_3(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_2(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_4(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_3(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_5(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_4(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_6(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_5(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_7(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_6(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_8(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_7(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_9(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_8(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_a(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_9(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_b(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_a(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_c(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_b(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_d(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_c(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_e(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_d(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_f(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_e(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_g(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_f(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_h(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_g(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_i(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_h(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_j(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_i(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_k(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_j(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_l(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_k(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_m(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_l(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_n(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_m(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_o(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_n(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_p(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_o(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_q(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_p(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_r(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_q(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_s(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_r(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_t(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_s(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_u(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_t(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_v(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_u(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_w(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_v(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_x(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_w(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_y(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_x(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_z(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_y(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_A(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_z(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_B(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_A(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_C(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_B(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_D(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_C(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_E(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_D(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_F(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_E(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_G(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_F(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_H(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_G(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_I(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_H(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_J(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_I(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_K(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_J(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_L(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_K(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_M(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_L(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_N(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_M(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_O(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_N(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_P(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_O(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_Q(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_P(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_R(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_Q(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_S(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_R(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_T(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_S(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_U(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_T(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_V(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_U(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_W(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_V(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_X(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_W(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_Y(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_X(y,__VA_ARGS__))
#define ROCKSDB_PP_CAT_Z(x,y,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT_Y(y,__VA_ARGS__))
///@}

///@param x at least one arg x
#define ROCKSDB_PP_CAT(x,...) ROCKSDB_PP_CAT2(x,ROCKSDB_PP_CAT2 \
       (ROCKSDB_PP_CAT_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__))


///@{
#define ROCKSDB_PP_JOIN_0()
#define ROCKSDB_PP_JOIN_1(x)       x
#define ROCKSDB_PP_JOIN_2(x,y)     x y
#define ROCKSDB_PP_JOIN_3(x,y,z)   x y z
#define ROCKSDB_PP_JOIN_4(x,y,z,w) x y z w
#define ROCKSDB_PP_JOIN_5(x,y,...) x ROCKSDB_PP_JOIN_4(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_6(x,y,...) x ROCKSDB_PP_JOIN_5(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_7(x,y,...) x ROCKSDB_PP_JOIN_6(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_8(x,y,...) x ROCKSDB_PP_JOIN_7(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_9(x,y,...) x ROCKSDB_PP_JOIN_8(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_a(x,y,...) x ROCKSDB_PP_JOIN_9(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_b(x,y,...) x ROCKSDB_PP_JOIN_a(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_c(x,y,...) x ROCKSDB_PP_JOIN_b(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_d(x,y,...) x ROCKSDB_PP_JOIN_c(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_e(x,y,...) x ROCKSDB_PP_JOIN_d(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_f(x,y,...) x ROCKSDB_PP_JOIN_e(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_g(x,y,...) x ROCKSDB_PP_JOIN_f(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_h(x,y,...) x ROCKSDB_PP_JOIN_g(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_i(x,y,...) x ROCKSDB_PP_JOIN_h(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_j(x,y,...) x ROCKSDB_PP_JOIN_i(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_k(x,y,...) x ROCKSDB_PP_JOIN_j(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_l(x,y,...) x ROCKSDB_PP_JOIN_k(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_m(x,y,...) x ROCKSDB_PP_JOIN_l(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_n(x,y,...) x ROCKSDB_PP_JOIN_m(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_o(x,y,...) x ROCKSDB_PP_JOIN_n(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_p(x,y,...) x ROCKSDB_PP_JOIN_o(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_q(x,y,...) x ROCKSDB_PP_JOIN_p(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_r(x,y,...) x ROCKSDB_PP_JOIN_q(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_s(x,y,...) x ROCKSDB_PP_JOIN_r(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_t(x,y,...) x ROCKSDB_PP_JOIN_s(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_u(x,y,...) x ROCKSDB_PP_JOIN_t(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_v(x,y,...) x ROCKSDB_PP_JOIN_u(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_w(x,y,...) x ROCKSDB_PP_JOIN_v(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_x(x,y,...) x ROCKSDB_PP_JOIN_w(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_y(x,y,...) x ROCKSDB_PP_JOIN_x(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_z(x,y,...) x ROCKSDB_PP_JOIN_y(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_A(x,y,...) x ROCKSDB_PP_JOIN_z(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_B(x,y,...) x ROCKSDB_PP_JOIN_A(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_C(x,y,...) x ROCKSDB_PP_JOIN_B(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_D(x,y,...) x ROCKSDB_PP_JOIN_C(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_E(x,y,...) x ROCKSDB_PP_JOIN_D(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_F(x,y,...) x ROCKSDB_PP_JOIN_E(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_G(x,y,...) x ROCKSDB_PP_JOIN_F(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_H(x,y,...) x ROCKSDB_PP_JOIN_G(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_I(x,y,...) x ROCKSDB_PP_JOIN_H(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_J(x,y,...) x ROCKSDB_PP_JOIN_I(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_K(x,y,...) x ROCKSDB_PP_JOIN_J(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_L(x,y,...) x ROCKSDB_PP_JOIN_K(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_M(x,y,...) x ROCKSDB_PP_JOIN_L(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_N(x,y,...) x ROCKSDB_PP_JOIN_M(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_O(x,y,...) x ROCKSDB_PP_JOIN_N(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_P(x,y,...) x ROCKSDB_PP_JOIN_O(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_Q(x,y,...) x ROCKSDB_PP_JOIN_P(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_R(x,y,...) x ROCKSDB_PP_JOIN_Q(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_S(x,y,...) x ROCKSDB_PP_JOIN_R(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_T(x,y,...) x ROCKSDB_PP_JOIN_S(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_U(x,y,...) x ROCKSDB_PP_JOIN_T(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_V(x,y,...) x ROCKSDB_PP_JOIN_U(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_W(x,y,...) x ROCKSDB_PP_JOIN_V(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_X(x,y,...) x ROCKSDB_PP_JOIN_W(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_Y(x,y,...) x ROCKSDB_PP_JOIN_X(y,__VA_ARGS__)
#define ROCKSDB_PP_JOIN_Z(x,y,...) x ROCKSDB_PP_JOIN_Y(y,__VA_ARGS__)
///@}

///@param x at least one arg x
#define ROCKSDB_PP_JOIN(x,...) x ROCKSDB_PP_CAT2 \
       (ROCKSDB_PP_JOIN_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(__VA_ARGS__)

///@{
///@param m map function
///@param c context
#define ROCKSDB_PP_MAP_0(m,c)
#define ROCKSDB_PP_MAP_1(m,c,x)     m(c,x)
#define ROCKSDB_PP_MAP_2(m,c,x,y)   m(c,x),m(c,y)
#define ROCKSDB_PP_MAP_3(m,c,x,y,z) m(c,x),m(c,y),m(c,z)
#define ROCKSDB_PP_MAP_4(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_3(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_5(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_4(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_6(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_5(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_7(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_6(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_8(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_7(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_9(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_8(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_a(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_9(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_b(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_a(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_c(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_b(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_d(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_c(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_e(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_d(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_f(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_e(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_g(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_f(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_h(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_g(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_i(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_h(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_j(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_i(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_k(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_j(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_l(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_k(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_m(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_l(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_n(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_m(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_o(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_n(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_p(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_o(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_q(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_p(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_r(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_q(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_s(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_r(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_t(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_s(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_u(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_t(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_v(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_u(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_w(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_v(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_x(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_w(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_y(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_x(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_z(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_y(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_A(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_z(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_B(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_A(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_C(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_B(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_D(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_C(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_E(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_D(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_F(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_E(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_G(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_F(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_H(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_G(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_I(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_H(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_J(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_I(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_K(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_J(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_L(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_K(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_M(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_L(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_N(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_M(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_O(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_N(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_P(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_O(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_Q(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_P(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_R(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_Q(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_S(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_R(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_T(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_S(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_U(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_T(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_V(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_U(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_W(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_V(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_X(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_W(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_Y(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_X(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_Z(m,c,x,...) m(c,x),ROCKSDB_PP_MAP_Y(m,c,__VA_ARGS__)
///@}

/// @param map map function, can be a macro, called as map(ctx,arg)
/// @param ctx context
/// @param ... arg list to apply map function: map(ctx,arg)
/// @returns comma seperated list: map(ctx,arg1), map(ctx,arg2), ...
/// @note at least zero args
#define ROCKSDB_PP_MAP(map,ctx,...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_PP_MAP_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(map,ctx,##__VA_ARGS__)

///@{
///@param m map(c,x,y) is a 3-arg function
///@param c context
#define ROCKSDB_PP_MAP_PAIR_0(m,c)
#define ROCKSDB_PP_MAP_PAIR_2(m,c,x,y)     m(c,x,y)
#define ROCKSDB_PP_MAP_PAIR_4(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_2(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_6(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_4(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_8(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_6(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_a(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_8(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_c(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_a(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_e(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_c(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_g(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_e(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_i(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_g(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_k(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_i(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_m(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_k(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_o(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_m(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_q(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_o(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_s(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_q(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_u(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_s(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_w(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_u(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_y(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_w(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_A(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_y(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_C(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_A(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_E(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_C(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_G(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_E(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_I(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_G(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_K(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_I(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_M(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_K(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_O(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_M(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_Q(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_O(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_S(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_Q(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_U(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_S(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_W(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_U(m,c,__VA_ARGS__)
#define ROCKSDB_PP_MAP_PAIR_Y(m,c,x,y,...) m(c,x,y),ROCKSDB_PP_MAP_PAIR_W(m,c,__VA_ARGS__)
///@}

/// @param map map(c,x,y) 3-arg, function, can be a macro, called as map(ctx,x,y)
/// @param ctx context
/// @param ... arg list to apply map function: map(ctx,x,y), arg list len must be even
/// @returns comma seperated list: map(ctx,x1,y1), map(ctx,x2,y2), ...
/// @note at least zero args
#define ROCKSDB_PP_MAP_PAIR(map,ctx,...) ROCKSDB_PP_CAT2 \
       (ROCKSDB_PP_MAP_PAIR_,ROCKSDB_PP_ARG_N(__VA_ARGS__))(map,ctx,##__VA_ARGS__)

///@{
///@param g group function g(m,c,x) where x is parented such as: (1,2,3)
///@param m map function
///@param c context
#define ROCKSDB_PP_GRP_MAP_0(g,m,c)
#define ROCKSDB_PP_GRP_MAP_1(g,m,c,x)     g(m,c,x)
#define ROCKSDB_PP_GRP_MAP_2(g,m,c,x,y)   g(m,c,x),g(m,c,y)
#define ROCKSDB_PP_GRP_MAP_3(g,m,c,x,y,z) g(m,c,x),g(m,c,y),g(m,c,z)
#define ROCKSDB_PP_GRP_MAP_4(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_3(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_5(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_4(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_6(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_5(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_7(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_6(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_8(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_7(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_9(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_8(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_a(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_9(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_b(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_a(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_c(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_b(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_d(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_c(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_e(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_d(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_f(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_e(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_g(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_f(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_h(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_g(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_i(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_h(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_j(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_i(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_k(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_j(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_l(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_k(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_m(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_l(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_n(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_m(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_o(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_n(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_p(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_o(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_q(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_p(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_r(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_q(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_s(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_r(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_t(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_s(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_u(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_t(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_v(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_u(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_w(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_v(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_x(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_w(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_y(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_x(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_z(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_y(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_A(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_z(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_B(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_A(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_C(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_B(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_D(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_C(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_E(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_D(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_F(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_E(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_G(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_F(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_H(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_G(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_I(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_H(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_J(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_I(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_K(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_J(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_L(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_K(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_M(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_L(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_N(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_M(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_O(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_N(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_P(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_O(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_Q(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_P(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_R(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_Q(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_S(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_R(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_T(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_S(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_U(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_T(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_V(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_U(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_W(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_V(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_X(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_W(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_Y(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_X(g,m,c,__VA_ARGS__)
#define ROCKSDB_PP_GRP_MAP_Z(g,m,c,x,...) g(m,c,x),ROCKSDB_PP_GRP_MAP_Y(g,m,c,__VA_ARGS__)
///@}

///@param parented is parented arglist such as (1,2,3)
#define ROCKSDB_PP_GRP_MAP_ONE_GROUP(map,ctx,parented) \
   ROCKSDB_PP_APPLY( \
     ROCKSDB_PP_CAT2(ROCKSDB_PP_MAP_,ROCKSDB_PP_ARG_N parented), \
     map, ctx, ROCKSDB_PP_REMOVE_PARENT_AUX parented)

///@param grp group function grp(map,ctx,one_parented_arglist)
///           in which one_parented_arglist seems like (1,2,3)
///@param map map function
///@returns (1,2),(3),(4,5) -> g(m,c,(1,2)),g(m,c,(3)),g(m,c,(4,5))
#define ROCKSDB_PP_GRP_MAP(grp,map,ctx,...) \
    ROCKSDB_PP_CAT2(ROCKSDB_PP_GRP_MAP_,ROCKSDB_PP_ARG_N(__VA_ARGS__)) \
    (grp,map,ctx,##__VA_ARGS__)

///@brief easy use, like ROCKSDB_PP_MAP, but __VA_ARGS__ seems like (1,2),(3),(4,5)
///@returns (1,2),(3),(4,5) -> m(c,1),m(c,2),m(c,3),m(c,4),m(c,5)
#define ROCKSDB_PP_BIG_MAP(map,ctx,...) \
  ROCKSDB_PP_GRP_MAP(ROCKSDB_PP_GRP_MAP_ONE_GROUP,map,ctx,##__VA_ARGS__)

/// @param dummy unused param 'context'
#define ROCKSDB_PP_IDENTITY_MAP_OP(dummy, x) x

/// @param prefix is param 'c'(context) in ROCKSDB_PP_MAP
#define ROCKSDB_PP_PREPEND(prefix, x) prefix x

/// @param prefix is param 'c'(context) in ROCKSDB_PP_MAP
#define ROCKSDB_PP_APPEND(suffix, x) x suffix

/// @{ ROCKSDB_PP_STR is a use case of ROCKSDB_PP_MAP
/// macro ROCKSDB_PP_STR_2 is the 'map' function
/// context of ROCKSDB_PP_STR_2 is dummy
///
/// ROCKSDB_PP_STR(a)     will produce: "a"
/// ROCKSDB_PP_STR(a,b,c) will produce: "a", "b", "c"
/// so ROCKSDB_PP_STR is a generic stringize macro
#define ROCKSDB_PP_STR_1(c,x) #x
#define ROCKSDB_PP_STR_2(c,x) ROCKSDB_PP_STR_1(c,x)

/// @note context for calling ROCKSDB_PP_MAP is dummy(noted as '~')
/// @param ... arg list to be stringized
#define ROCKSDB_PP_STR(...) ROCKSDB_PP_MAP(ROCKSDB_PP_STR_2,~, __VA_ARGS__)
/// @}

///@param arg is a list with parent: (1,2,3)
///@param ctx ignored
///@returns 1,2,3 -- parents are removed
#define ROCKSDB_PP_FLATTEN_ONE(ctx,arg) ROCKSDB_PP_REMOVE_PARENT(arg)

///@param __VA_ARGS__ should be  (1,2,3), (4,5,6), ...
///@returns 1,2,3,4,5,6,...
#define ROCKSDB_PP_FLATTEN(...) \
   ROCKSDB_PP_MAP(ROCKSDB_PP_FLATTEN_ONE, ~, __VA_ARGS__)

///@param arg is a list with parent: (1,2,3)
///@param ctx ignored
///@returns "1,2,3" -- parents are removed then convert to string
#define ROCKSDB_PP_STR_FLATTEN_ONE(ctx, arg) ROCKSDB_PP_STR_FLATTEN_ONE_AUX arg
#define ROCKSDB_PP_STR_FLATTEN_ONE_AUX(...) #__VA_ARGS__

///@param __VA_ARGS__ = (1,2,3), (4,5,6), ...
///@returns "1,2,3", "4,5,6", ...
#define ROCKSDB_PP_STR_FLATTEN(...) \
   ROCKSDB_PP_MAP(ROCKSDB_PP_STR_FLATTEN_ONE, ~, __VA_ARGS__)

#if defined(__GNUC__) || (defined(__MWERKS__) && (__MWERKS__ >= 0x3000)) || \
   (defined(__ICC) && (__ICC >= 600)) || defined(__ghs__) || defined(__clang__)

# define ROCKSDB_FUNC __PRETTY_FUNCTION__
# define ROCKSDB_FLATTEN __attribute__((flatten))

#elif defined(__DMC__) && (__DMC__ >= 0x810)

# define ROCKSDB_FUNC __PRETTY_FUNCTION__

#elif defined(__FUNCSIG__)

# define ROCKSDB_FUNC __FUNCSIG__

#elif (defined(__INTEL_COMPILER) && (__INTEL_COMPILER >= 600)) || (defined(__IBMCPP__) && (__IBMCPP__ >= 500))

# define ROCKSDB_FUNC __FUNCTION__

#elif defined(__BORLANDC__) && (__BORLANDC__ >= 0x550)

# define ROCKSDB_FUNC __FUNC__

#elif defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901)

# define ROCKSDB_FUNC __func__

#elif defined(__cplusplus) && (__cplusplus >= 201103)

# define ROCKSDB_FUNC __func__

#else

# define ROCKSDB_FUNC "(unknown)"

#endif

#if !defined(ROCKSDB_FLATTEN)
# define ROCKSDB_FLATTEN
#endif

/////////////////////////////////////////////////////////////////////////////////////////////////

#include "port/likely.h"

#define ROCKSDB_DIE(fmt, ...) \
	do { \
        fprintf(stderr, "%s:%d: %s: die: " fmt " !\n", \
                __FILE__, __LINE__, ROCKSDB_FUNC, ##__VA_ARGS__); \
        abort(); } while (0)

/// VERIFY indicate runtime assert in release build
#define ROCKSDB_VERIFY_F_IMP(expr, fmt, ...) \
    do { if (UNLIKELY(!(expr))) { \
        fprintf(stderr, "%s:%d: %s: verify(%s) failed" fmt " !\n", \
                __FILE__, __LINE__, ROCKSDB_FUNC, #expr, ##__VA_ARGS__); \
        abort(); }} while (0)

#define ROCKSDB_VERIFY_F(expr, fmt, ...) \
        ROCKSDB_VERIFY_F_IMP(expr, ": " fmt, ##__VA_ARGS__)

#if defined(_DEBUG) || defined(DEBUG) || !defined(NDEBUG)
#	define ROCKSDB_IF_DEBUG(Then, Else)  Then
#	define ROCKSDB_ASSERT_F ROCKSDB_VERIFY_F
#	define ROCKSDB_VERIFY assert
#else
#	define ROCKSDB_IF_DEBUG(Then, Else)  Else
#	define ROCKSDB_ASSERT_F(...)
#	define ROCKSDB_VERIFY(expr) ROCKSDB_VERIFY_F_IMP(expr, "")
#endif

#define ROCKSDB_ASSERT_LT(x,y) ROCKSDB_ASSERT_F(x <  y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_ASSERT_GT(x,y) ROCKSDB_ASSERT_F(x >  y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_ASSERT_LE(x,y) ROCKSDB_ASSERT_F(x <= y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_ASSERT_GE(x,y) ROCKSDB_ASSERT_F(x >= y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_ASSERT_EQ(x,y) ROCKSDB_ASSERT_F(x == y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_ASSERT_NE(x,y) ROCKSDB_ASSERT_F(x != y, "%lld %lld", (long long)(x), (long long)(y))

// _EZ: Equal To Zero
#define ROCKSDB_ASSERT_EZ(x) ROCKSDB_ASSERT_F(x == 0, "%lld", (long long)(x))

// _AL: Align, _NA: Not Align
#define ROCKSDB_ASSERT_AL(x,a) ROCKSDB_ASSERT_F((x) % (a) == 0, "%lld %% %lld = %lld", (long long)(x), (long long)(a), (long long)((x) % (a)))
#define ROCKSDB_ASSERT_NA(x,a) ROCKSDB_ASSERT_F((x) % (a) != 0, x)

#define ROCKSDB_VERIFY_LT(x,y) ROCKSDB_VERIFY_F(x <  y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_VERIFY_GT(x,y) ROCKSDB_VERIFY_F(x >  y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_VERIFY_LE(x,y) ROCKSDB_VERIFY_F(x <= y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_VERIFY_GE(x,y) ROCKSDB_VERIFY_F(x >= y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_VERIFY_EQ(x,y) ROCKSDB_VERIFY_F(x == y, "%lld %lld", (long long)(x), (long long)(y))
#define ROCKSDB_VERIFY_NE(x,y) ROCKSDB_VERIFY_F(x != y, "%lld %lld", (long long)(x), (long long)(y))

// _EZ: Equal To Zero
#define ROCKSDB_VERIFY_EZ(x) ROCKSDB_VERIFY_F(x == 0, "%lld", (long long)(x))

// _AL: Align, _NA: Not Align
#define ROCKSDB_VERIFY_AL(x,a) ROCKSDB_VERIFY_F((x) % (a) == 0, "%lld %% %lld = %lld", (long long)(x), (long long)(a), (long long)((x) % (a)))
#define ROCKSDB_VERIFY_NA(x,a) ROCKSDB_VERIFY_F((x) % (a) != 0, "%lld", (long long)(x))

namespace ROCKSDB_NAMESPACE {
	template<class Func>
	class OnScopeExit {
		const Func& on_exit;
	public:
		OnScopeExit(const Func& f) : on_exit(f) {}
		~OnScopeExit() { on_exit(); }
	};

} // namespace ROCKSDB_NAMESPACE

#define ROCKSDB_SCOPE_EXIT(...) \
    auto ROCKSDB_PP_CAT2(func_on_exit_,__LINE__) = [&]() { __VA_ARGS__; }; \
    ROCKSDB_NAMESPACE::OnScopeExit< \
decltype(ROCKSDB_PP_CAT2(func_on_exit_,__LINE__))> \
         ROCKSDB_PP_CAT2(call_on_exit_,__LINE__)   \
        (ROCKSDB_PP_CAT2(func_on_exit_,__LINE__))

// clang-format on
